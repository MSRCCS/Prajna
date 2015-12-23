using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DistributedSGD
{
    class Arguments
    {
        public string TrainFile;
        public string TestFile;
        public int NumTrain = -1;
        public int NumTest = -1;
        public string Loss = "Hinge";
        public float LearningRate = 0.01f;
        public float L2 = 0.00001f;
        public float L1 = 0.000001f;
        public int NumEpochs = 100;
        public string ModelOut;
        public string Cluster = "local[2]";
        public int NumPartitions = 50;

        static void PrintUsage()
        {
            Console.WriteLine();
            Console.WriteLine(@"Usage: DistributedLogisticRegression.exe
    -train <TrainFile>
    [-test <TestFile>] 
    [-numTrain <NumberOfExamplesToUseForTraining> (defaults to all examples in TrainFile)  ] 
    [-numTest <NumberOfExamplesToUseForTesting> (defaults to all examples in TestFile)  ] 
    [-loss <Hinge | Logistic>] (defaults to Hinge)
    [-lr <LearningRate>] (defaults to 0.01)
    [-L2 <L2Factor>]  (defaults to 10^-5)
    [-L1 <L1Factor>] (defaults to 10^-6)
    [-epochs <NumberOfEpochs>] (defaults to 100)
    [-modelOut <OutputModelFile>] (defaults to not saving the trained model)
    [-cluster <ClusterFile>]  (defaults to ""local[2]"")
    [-numPartitions <NumberOfPartitionsPerNode>]  (defaults to 50)");
        }

        static internal Arguments Parse(string[] args)
        {
            var arguments = new Arguments();
            for (int i = 0; i < args.Length; i++)
            {
                switch (args[i].ToLower())
                {
                    case "-train":
                        i++;
                        arguments.TrainFile = args[i];
                        break;
                    case "-test":
                        i++;
                        arguments.TestFile = args[i];
                        break;
                    case "-numtrain":
                        i++;
                        if (!int.TryParse(args[i], out arguments.NumTrain))
                        {
                            Console.WriteLine("Error: NumTrain must be an int.");
                            PrintUsage();
                            return null;
                        }
                        break;
                    case "-numtest":
                        i++;
                        if (!int.TryParse(args[i], out arguments.NumTest))
                        {
                            Console.WriteLine("Error: NumTest must be an int.");
                            PrintUsage();
                            return null;
                        }
                        break;
                    case "-loss":
                        i++;
                        arguments.Loss = args[i];
                        break;
                    case "-lr":
                        i++;
                        if (!float.TryParse(args[i], out arguments.LearningRate))
                        {
                            Console.WriteLine("Error: LearningRate must be a float.");
                            PrintUsage();
                            return null;
                        }
                        break;
                    case "-l2":
                        i++;
                        if (!float.TryParse(args[i], out arguments.L2))
                        {
                            Console.WriteLine("Error: L2 must be a float.");
                            PrintUsage();
                            return null;
                        }
                        break;
                    case "-l1":
                        i++;
                        if (!float.TryParse(args[i], out arguments.L1))
                        {
                            Console.WriteLine("Error: L1 must be a float.");
                            PrintUsage();
                            return null;
                        }
                        break;
                    case "-epochs":
                        i++;
                        if (!int.TryParse(args[i], out arguments.NumEpochs))
                        {
                            Console.WriteLine("Error: NumEpochs must be an int.");
                            PrintUsage();
                            return null;
                        }
                        break;
                    case "-modelout":
                        i++;
                        arguments.ModelOut = args[i];
                        break;
                    case "-cluster":
                        i++;
                        arguments.Cluster = args[i];
                        break;
                    case "-numpartitions":
                        i++;
                        if (!int.TryParse(args[i], out arguments.NumPartitions))
                        {
                            Console.WriteLine("Error: NumPartitions must be an int.");
                            PrintUsage();
                            return null;
                        }
                        break;
                    default:
                        Console.WriteLine($"Error: Unrecognized argument: {args[i]}");
                        PrintUsage();
                        return null;
                }
            }
            if (arguments.TrainFile == null)
            {
                Console.WriteLine("Insuficient arguments. \"-train\" is mandatory");
                PrintUsage();
                return null;
            }
            return arguments;
        }
    }
}
