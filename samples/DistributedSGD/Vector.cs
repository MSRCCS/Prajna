using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DistributedSGD
{

    static class DenseVectorExtensions
    {
        public static float Dot(this float[] dense, SparseVector sparse)
        {
            Debug.Assert(dense.Length == sparse.Dimension);
            float acc = 0;
            var count = sparse.Count;
            var indices = sparse.Indices;
            for (int i = 0; i < indices.Length; i++)
            {
                if (i >= count)
                    break;
                var ix = indices[i];
                acc += dense[ix] * sparse.Values[i];
            }
            return acc;
        }

        public static void InplaceAddMultiply(this float[] dense, float factor, SparseVector sparse)
        {
            Debug.Assert(dense.Length == sparse.Dimension);
            var count = sparse.Count;
            var indices = sparse.Indices;
            for (int i = 0; i < indices.Length; i++)
            {
                if (i >= count)
                    break;
                var ix = indices[i];
                dense[ix] = dense[ix] + factor * sparse.Values[i];
            }
        }

        /// <summary>
        /// Used by L2 regularized SGD to push the weights in the direction of/contrary-to an example vector,
        /// by a given loss amount and learning rate, while shrinking all weights by a small factor
        /// to prevent overfitting. The factor comes from the definition of the regularized loss:
        /// L(w,e) =  ... + lambda/2 * norm-2(w)^2, whose deriviative is simply lambda * w.
        /// </summary>
        public static void InplaceAddMultiplyWithL2(this float[] dense, float learningRate, float loss, float L2, SparseVector sparse)
        {
            Debug.Assert(dense.Length == sparse.Dimension);
            var count = sparse.Count;
            var indices = sparse.Indices;
            var weightFactor = loss * learningRate;
            var L2Factor = (1.0f - L2) * learningRate;
            for (int i = 0; i < indices.Length; i++)
            {
                if (i >= count)
                    break;
                var ix = indices[i];
                var wi = dense[ix];
                dense[ix] = wi + (weightFactor * sparse.Values[i]) - (L2Factor * wi);
            }
        }

        /// <summary>
        /// Used by L2 + L1 regularized SGD to push the weights in the direction of/contrary-to an example vector.
        /// In addition to L2 regularization as above, we also shrinking all weights towards zero 
        /// by a small constant amount (L1), clamping at 0.0f. This is also to prevent overfitting, and has the effect 
        /// of doing some automatic feature selection, since most weights will end up being indeed 0.0 with a large
        /// enough L1. The amount comes from the (sub-)derivative of the L1 norm, which is just the constant "L1".
        /// </summary>
        public static void InplaceAddMultiplyWithL2AndL1(this float[] dense, float learningRate, float loss, float L2, float L1, SparseVector sparse)
        {
            Debug.Assert(dense.Length == sparse.Dimension);
            var count = sparse.Count;
            var indices = sparse.Indices;
            var weightFactor = loss * learningRate;
            var L2Factor = (1.0f - L2) * learningRate;
            var L1Factor = (L1) * learningRate;
            for (int i = 0; i < indices.Length; i++)
            {
                if (i >= count)
                    break;
                var ix = indices[i];
                var wi = dense[ix];
                var candidateWeightAfterL2 = wi + (weightFactor * sparse.Values[i]) - (L2Factor * wi);
                dense[ix] =
                    Math.Abs(candidateWeightAfterL2) < L1Factor ? 0.0f
                    : candidateWeightAfterL2 > 0.0f ? candidateWeightAfterL2 - L1Factor
                    : candidateWeightAfterL2 + L1Factor;
            }
        }

        public static void InplaceAdd(this float[] dense, float[] other)
        {
            for (var i = 0; i < dense.Length; i++)
                dense[i] += other[i];
        }
        public static void InplaceMultiplyScalar(this float[] dense, float factor)
        {
            for (int i = 0; i < dense.Length; i++)
                dense[i] = dense[i] * factor;
        }
    }


    /// <summary>
    /// Besides the usual Dimension, Indices and Values, our SparseVector keeps a count,
    /// which represents the number of entries in Indices and Values that should be considered
    /// "valid". The remaining should not be considered part of the vector, and the operations
    /// account for that. This allows a client to prevent GC thrashing be reusing the buffers
    /// associated with a SparseVector once it's no longer needed.
    /// </summary>
    [Serializable]
    internal class SparseVector
    {
        public SparseVector(int dimension, int[] indices, float[] values, int count)
        {
            Debug.Assert(indices.Length == values.Length);
            Debug.Assert(indices.Length >= count);
            Dimension = dimension;
            Indices = indices;
            Values = values;
            Count = count;
        }
        public SparseVector(int dimension) : this(dimension, new int[0], new float[0], 0) { }
        public int Dimension { get; set; }
        public int Count { get; private set; }
        public int[] Indices { get; private set; }
        public float[] Values { get; private set; }

        public SparseVector Clone()
        {
            var indices = new int[Count];
            Array.Copy(Indices, indices, Count);
            var values = new float[Count];
            Array.Copy(Values, values, Count);
            return new SparseVector(Dimension, indices, values, Count);
        }

        public void UpdateWith(SparseVector other)
        {
            Debug.Assert(Dimension == other.Dimension);
            if (Indices.Length < other.Count)
            {
                Indices = new int[other.Count];
                Values = new float[other.Count];
            }
            Array.Copy(other.Indices, Indices, other.Count);
            Array.Copy(other.Values, Values, other.Count);
            Count = other.Count;
        }

        public void InplaceMultiplyScalar(float factor)
        {
            for (int i = 0; i < Indices.Length; i++)
            {
                if (i >= Count)
                    break;
                Values[i] *= factor;
            }
        }

        static public void UpdateWithProduct(SparseVector dst, float factor, SparseVector other)
        {
            Debug.Assert(dst.Dimension == other.Dimension);
            if (dst.Indices.Length < other.Count)
            {
                dst.Indices = new int[other.Count];
                dst.Values = new float[other.Count];
            }
            dst.Count = other.Count;
            Array.Copy(other.Indices, dst.Indices, dst.Count);
            var values = dst.Values;
            for (int i = 0; i < values.Length; i++)
            {
                if (i >= dst.Count)
                    break;
                values[i] = factor * other.Values[i];
            }
        }
    }
}
