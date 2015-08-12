rm scipy-0.15.1.tar.gz
wget https://pypi.python.org/packages/source/s/scipy/scipy-0.15.1.tar.gz

rm -r scipy-0.15.1
tar -zxvf scipy-0.15.1.tar.gz


cd scipy-0.15.1

export BLAS=/opt/OpenBLAS/lib/libopenblas.so
export LAPACK=/opt/OpenBLAS/lib/libopenblas.so

export LIBRARY_PATH=$LIBRARY_PATH:/opt/OpenBLAS/lib

python setup.py install

cd ..

rm -r  scipy-0.15.1
rm scipy-0.15.1.tar.gz
