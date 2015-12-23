using System;
using System.Collections.Concurrent;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Prajna.Core;
using Prajna.Tools;
using Prajna.Api.CSharp;

namespace DistributedSGD
{
    class Program
    {
        /// <summary>
        /// Parse a line in LibSVM format into an Example while trying not th thrash the GC too much.
        /// LibSVM format is one example per line, tab separated, with the first column being the label,
        /// followed by any number of FeatureIndex:FeatureValue pairs, followed by a newline.
        /// Unfortunately, int/float.Parse requires a separate string (no index+offset version),
        /// so at least those we need to allocate (or write our own ParseInt/Float).
        /// </summary>
        private static Example ParseLine(int dimension, string line)
        {
            int spacePos = line.IndexOf(' ');
            float label = line.Substring(0, spacePos) == "0" ? 0.0f : 1.0f;
            var indexList = new List<int>();
            var valueList = new List<float>();
            int nextStart = spacePos + 1;
            while (nextStart != -1)
            {
                int colonPos = line.IndexOf(':', nextStart);
                string indexStr = line.Substring(nextStart, colonPos - nextStart);
                indexList.Add(int.Parse(indexStr));
                int valStart = colonPos + 1;
                int nextSpace = line.IndexOf(' ', valStart);
                int valEnd = nextSpace == -1 ? line.Length : nextSpace;
                string valueStr = line.Substring(valStart, valEnd - valStart);
                valueList.Add(float.Parse(valueStr));
                nextStart = nextSpace == -1 ? -1 : nextSpace + 1;
            }
            var indices = new int[indexList.Count];
            indexList.CopyTo(indices);
            var values = new float[valueList.Count];
            valueList.CopyTo(values);
            return new Example(label, new SparseVector(dimension, indices, values, indices.Length));
        }

        private static WeightsAndBias AddWeights(WeightsAndBias wb1, WeightsAndBias wb2) 
        {
            wb1.Weights.InplaceAdd(wb2.Weights);
            wb1.Bias += wb2.Bias;
            return wb1;
        }

        /// <summary>
        /// An object that runs a single epoch of an SGD and can evaluate its accuracy.
        /// </summary>
        /// <typeparam name="Dataset"></typeparam>
        /// <typeparam name="Params"></typeparam>
        interface ISGDEpoch<Dataset, Params>
        {
            Params RunEpoch(Dataset dataset, ISGDModel<Params> model, Params curWeights);
            float GetAccuracy(Dataset trainSet, ISGDModel<Params> model, Params curWeights);
        }

        /// <summary>
        /// A Prajna DSet-based implementation of ISGDEpoch, for by type of model paramenters.
        /// </summary>
        class DistributedSGD<Params> : ISGDEpoch<DSet<Example>, Params>
        {
            static public readonly DistributedSGD<Params> Instance = new DistributedSGD<Params>();
            public float GetAccuracy(DSet<Example> dataset, ISGDModel<Params> model, Params curParams)
            {
                int hits = dataset.Fold((c, ex) => model.Predict(curParams, ex) == ex.Label ? c + 1 : c, (c1, c2) => c1 + c2, 0);
                return (float)hits / dataset.Count();
            }

            public Params RunEpoch(DSet<Example> trainSet, ISGDModel<Params> model, Params curParams)
            {
                Params accumulatedParams =
                    trainSet.Fold(
                        (wb, e) => { return model.Update(wb, e); },
                        model.AddParams,
                        curParams);
                var averagedParams = model.ScaleParams(accumulatedParams, (float)(1.0 / trainSet.NumPartitions));
                return averagedParams;
            }
        }

        /// <summary>
        /// A single-threaded SGD Epoch, for reference, speed comparison, and test generality.
        /// </summary>
        class LocalSingleThreadedSGD : ISGDEpoch<Example[], WeightsAndBias>
        {
            static public readonly LocalSingleThreadedSGD Instance = new LocalSingleThreadedSGD();
            public float GetAccuracy(Example[] dataset, ISGDModel<WeightsAndBias> model, WeightsAndBias curWeights)
            {
                int hits = dataset.Sum(ex => model.Predict(curWeights, ex) == ex.Label ? 1 : 0);
                return (float)hits / dataset.Length;
            }

            public WeightsAndBias RunEpoch(Example[] trainSet, ISGDModel<WeightsAndBias> model, WeightsAndBias curWeights)
            {
                foreach (var e in trainSet)
                {
                    model.Update(curWeights, e);
                }
                return curWeights;
            }
        }

        /// <summary>
        /// Runs the outer loop of an SGD, parameterized by type of dataset (e.g.: Example[] or DSet<Example>)
        /// and type of model Params (currently only WeightsAndBias, but could be anything, such as a big neural net).
        /// The epochRunner runs each epoch and decides the strategy to run the epoch (distributed, sequential, multi-threaded),
        /// and it is also used to evaluates the accuracy after each 10 epochs.
        /// The model is passed to the epochRunner to the the individual updates, and so long as the param types match, any model
        /// can be used with any epoch runner. This is why we require each model to be able to average its parameters.
        /// To do: Use a validation set based criterion to stop, rather than number of epochs.
        /// </summary>
        private static Params RunSGD<Dataset, Params>(Dataset trainSet, Dataset testSet, ISGDModel<Params> model, Params initialParams, ISGDEpoch<Dataset, Params> epochRunner, int numEpochs)
        {
            var weightsAndBias = initialParams;
            var sw = System.Diagnostics.Stopwatch.StartNew();
            for (int epoch = 0; epoch < numEpochs; epoch++)
            {
                sw.Restart();
                Params epochResult = epochRunner.RunEpoch(trainSet, model, weightsAndBias);
                Console.WriteLine($"Epoch {epoch}. took: {sw.Elapsed}");

                if (epoch % 10 == 0)
                {
                    sw.Restart();
                    float trainAcc = epochRunner.GetAccuracy(trainSet, model, epochResult);
                    Console.WriteLine($"Train Accuracy: {trainAcc}");
                    if (testSet != null)
                    {
                        float testAcc = epochRunner.GetAccuracy(testSet, model, epochResult);
                        Console.WriteLine($"Test Accuracy: {testAcc}");
                    }
                    Console.WriteLine($"Evaluating accuracies took: {sw.Elapsed}.");
                    Console.WriteLine();
                }
                weightsAndBias = epochResult;
            }
            return weightsAndBias;
        }

        /// <summary>
        /// The "prior" of a dataset is simply the number of positive examples divided by the size of the dataset.
        /// </summary>
        /// <param name="examples"></param>
        /// <returns></returns>
        private static float GetPrior(DSet<Example> examples)
        {
            long hits = examples.Fold((c, ex) => ex.Label == 1.0f ? c + 1 : c, (c1, c2) => c1 + c2, 0);
            return (float)hits / examples.Count();
        }

        static void SetDimension(DSet<Example> ds, int dim)
        {
            ds.Iter(ex => ex.Features.Dimension = dim);
        }

        /// <summary>
        /// Loads a DSet in parallel by splitting the file int numPartitions * cluster.NumNodes, 
        /// finding the next separator (newline), and reading up to maxLines of each chunk in parallel.
        /// The file should be in LibSVM format, i.e.: tab separated with the first column being the label,
        /// followed by any number of FeatureIndex:FeatureValue pairs, followed by a newline.
        /// The dimension can be passed for convenience, but can also be set by the client later, so long
        /// as this is done before trying to any operations on the Example sparse vectors.
        /// </summary>
        private static DSet<Example> LoadDSet(Cluster cluster, string file, int numPartitions, int maxLines, int dimension)
        {
            int numNodes = cluster.NumNodes;
            var trainSet =
                new DSet<Example>() { Cluster = cluster, Name = "trainSet", NumParallelExecution = numPartitions, SerializationLimit = int.MaxValue }
                .SourceN(numPartitions, i =>
                {
                    // The dataset is divided in chunks by raw byte position,
                    // and each partition reads from the next newline until either
                    // (a) maxLines / numPartions are read or
                    // (b) it has read bast the raw start pos of the next partition
                    long trainFileSize = new FileInfo(file).Length;
                    long byteChunkSize = trainFileSize / (numPartitions * numNodes);
                    int maxLinesPerPartition = maxLines / (numPartitions * numNodes);

                    var rawStartPos = i * byteChunkSize;
                    var stream = new BinaryReader(File.OpenRead(file));
                    if (i != 0)
                    {
                        stream.BaseStream.Seek(rawStartPos, SeekOrigin.Begin);
                        // position stream after next newline
                        while (stream.ReadChar() != '\n')
                            ;
                    }
                    var nextPartRawStartPos = rawStartPos + byteChunkSize;
                    var reader = new StreamReader(stream.BaseStream);
                    string line;
                    var lines = new List<Example>();
                    while (stream.BaseStream.Position < nextPartRawStartPos
                        && lines.Count < maxLinesPerPartition
                        && (line = reader.ReadLine()) != null)
                    {
                        lines.Add(ParseLine(dimension, line));
                    }
                    return lines;
                })
                .CacheInMemory();
            return trainSet;
        }

        static void Main(string[] args)
        {
            Arguments arg;
            if ((arg = Arguments.Parse(args)) == null)
                return;

            var sw = System.Diagnostics.Stopwatch.StartNew();
            Console.Write("Initializing/Connecting to cluster... ");
            Logger.ParseArgs(new string[] { "-log", "DistributedSGD.log" });
            Prajna.Core.Environment.Init();

            var cluster = new Cluster(arg.Cluster);

            Console.WriteLine($"done. (took: {sw.Elapsed})");
            sw.Restart();
            Console.Write("Loading dataset(s)... ");
            DSet<Example> trainSet = LoadDSet(cluster, arg.TrainFile, arg.NumPartitions, arg.NumTrain > 0 ? arg.NumTrain : int.MaxValue, 0);
            DSet<Example> testSet = arg.TestFile == null ? null : LoadDSet(cluster, arg.TestFile, arg.NumPartitions, arg.NumTest > 0 ? arg.NumTest : int.MaxValue, 0);
            Func<DSet<Example>, int> getDimension = ds => ds.Fold((max, ex) => Math.Max(ex.Features.Indices.Max(), max), Math.Max, 0) + 1;
            var dimension = Math.Max(getDimension(trainSet), arg.TestFile == null ? 0 : getDimension(testSet));
            Console.WriteLine($"done. (took: {sw.Elapsed})");

            SetDimension(trainSet, dimension);
            Console.WriteLine($"Train Count: {trainSet.Count()}");
            float trainPrior = GetPrior(trainSet);
            Console.WriteLine($"Train Prior: {trainPrior}");

            if (arg.TestFile != null)
            {
                SetDimension(testSet, dimension);
                Console.WriteLine($"Test Count: {testSet.Count()}");
                float testPrior = GetPrior(testSet);
                Console.WriteLine($"Test Prior: {testPrior}");
            }

            ILossFunction loss;
            switch (arg.Loss.ToLower())
            {
                case "logistic":
                    loss = new LogisticLoss();
                    break;
                case "hinge":
                    loss = new HingeLoss();
                    break;
                default:
                    Console.WriteLine($"Unrecognized loss function: {arg.Loss}. Supported values: Hinge, Logistic.");
                    return;
            }

            if (arg.ModelOut != null)
            {
                if (!Directory.Exists(Path.GetDirectoryName(arg.ModelOut)))
                {
                    Console.WriteLine($"Directory {Path.GetDirectoryName(arg.ModelOut)} not found.");
                    return;
                }
            }
            var model = new LinearModel(new HingeLoss(), arg.LearningRate, arg.L2, arg.L1);
            var initialParams = new WeightsAndBias(new float[dimension], 0.0f);
            WeightsAndBias finalParams = RunSGD(trainSet, testSet, model, initialParams, DistributedSGD<WeightsAndBias>.Instance, arg.NumEpochs);
            if (arg.ModelOut != null)
            {
                Console.WriteLine();
                Console.Write("Done training. Saving model... ");
                sw.Restart();
                using (var writer = new StreamWriter(arg.ModelOut))
                {
                    writer.WriteLine("Dimension:");
                    writer.WriteLine(finalParams.Weights.Length);
                    writer.WriteLine("Weights:");
                    foreach (var w in finalParams.Weights)
                        writer.WriteLine(w);
                    writer.WriteLine("Bias:");
                    writer.WriteLine(finalParams.Bias);
                }
                Console.WriteLine($"done. (took: {sw.Elapsed})");
            }
        }
    }
}
