sudo apt-get -y install git python-dev gfortran make

git clone https://github.com/xianyi/OpenBLAS.git
cd OpenBLAS
make FC=gfortran
sudo make install


#//Set up how many threads OpenBLAS can use, the more the faster
export OPENBLAS_NUM_THREADS=8
cd ..
