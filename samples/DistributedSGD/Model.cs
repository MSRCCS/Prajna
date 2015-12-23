using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DistributedSGD
{
    [Serializable]
    internal class Example
    {
        public Example(float label, SparseVector features)
        {
            Label = label;
            Features = features;
        }
        /// <summary>
        /// The Label for this example. For binary classification, negative is represented
        /// by 0.0 and positive by 1.0.
        /// </summary>
        public float Label;

        public SparseVector Features;
    }

    /// <summary>
    /// The loss function determines how much to "punish" mistakes in the algorithm, given a score and a label.
    /// For optimization, we don't need the actual value of function, just its derivative.
    /// It may be useful to have the actual value to track the optimization goal in the future,
    /// but for now we keep just the derivative for simplicity.
    /// </summary>
    interface ILossFunction
    {
        /// <summary>
        /// Computes the derivative of the loss function with respect to the prediction, given a label.
        /// </summary>
        /// <param name="score">The score returned by the underlying model.</param>
        /// <param name="label">For binary classification, encoded as 0.0 for negative or 1.0 for positive</param>
        /// <returns></returns>
        float GetDerivative(float score, float label);
    }

    [Serializable]
    class LogisticLoss : ILossFunction
    {
        public float GetDerivative(float score, float label) => (float)(1.0 / (1.0 + Math.Exp(-score))) - label;
    }

    [Serializable]
    class HingeLoss : ILossFunction
    {
        public float GetDerivative(float score, float label)
        {
            var y = label == 1.0f ? 1.0f : -1.0f;
            return score * y > 1.0f ? 0.0f : -y;
        }
    }

    /// <summary>
    /// An ISGDModel mainly does a single-example update of a parameterized model.
    /// It must also be able to predict in the same label encoding as Example, 
    /// add and scale parameters, for averaging.
    /// Notice that the parameters themselves are managed separately from the model,
    /// and passed in as needed.
    /// It is encouraged for an ISGDModel to "reuse" the params objects passed AddParams
    /// and ScaleParams, since they are guaranteed not to be used after the calls.
    /// </summary>
    /// <typeparam name="Params"></typeparam>
    interface ISGDModel<Params>
    {
        /// <summary>
        /// Update the model for a new example.
        /// </summary>
        Params Update(Params p, Example e);

        /// <summary>
        /// Returns the model prediction for the given example.
        /// For binary classification, the return should be 0.0 or 1.0.
        /// </summary>
        float Predict(Params p, Example e);

        /// <summary>
        /// Add the model parameters, needed for parallel averaging.
        /// Parameters are guaranteed not to be reused, so it is encouraged
        /// to add inplace to p1 and return it.
        /// </summary>
        Params AddParams(Params p1, Params p2);

        /// <summary>
        /// Scale the model parameters, needed for averaging.
        /// Parameters are guaranteed not to be reused, so it is encouraged
        /// to scale ps inplace and return it.
        /// </summary>
        Params ScaleParams(Params ps, float factor);
    }


    /// <summary>
    /// Weights and bias for a linear model.
    /// We keep the bias separate because it may be beneficial to update it
    /// on a slower schedule than the main weights, as features are only
    /// only updated on a fraction of examples, but the bias is always updated.
    /// </summary>
    [Serializable]
    internal struct WeightsAndBias
    {
        public WeightsAndBias(float[] weights, float bias)
        {
            Weights = weights;
            Bias = bias;
        }
        public float[] Weights;
        public float Bias;
    }

    /// <summary>
    /// A linear model updater with a pluggable loss function, and L2 and L1 regularization.
    /// </summary>
    [Serializable]
    class LinearModel : ISGDModel<WeightsAndBias>
    {
        private ILossFunction _loss;
        private float _learningRate;
        private float _L2;
        private float _L1;

        public LinearModel(ILossFunction loss, float learningRate, float L2, float L1)
        {
            _loss = loss;
            _learningRate = learningRate;
            _L2 = L2;
            _L1 = L1;
        }

        private float Score(WeightsAndBias wb, Example e) => wb.Weights.Dot(e.Features) + wb.Bias;

        public WeightsAndBias Update(WeightsAndBias wb, Example example)
        {
            float DLoss = _loss.GetDerivative(Score(wb, example), example.Label);
            if (_L2 == 0.0f)
                wb.Weights.InplaceAddMultiply(-DLoss * _learningRate, example.Features);
            else if (_L1 == 0.0f)
                wb.Weights.InplaceAddMultiplyWithL2(_learningRate, -DLoss, _L2, example.Features);
            else
                wb.Weights.InplaceAddMultiplyWithL2AndL1(_learningRate, -DLoss, _L2, _L1, example.Features);
            wb.Bias += -_learningRate * DLoss;
            return wb;
        }

        public float Predict(WeightsAndBias wb, Example e) => Score(wb, e) >= 0.0f ? 1.0f : 0.0f;

        public WeightsAndBias AddParams(WeightsAndBias p1, WeightsAndBias p2)
        {
            p1.Weights.InplaceAdd(p2.Weights);
            p1.Bias += p2.Bias;
            return p1;
        }

        public WeightsAndBias ScaleParams(WeightsAndBias ps, float factor)
        {
            ps.Weights.InplaceMultiplyScalar(factor);
            ps.Bias *= factor;
            return ps;
        }
    }

}
