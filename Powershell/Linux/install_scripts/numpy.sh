rm numpy-1.9.2.tar.gz
wget https://pypi.python.org/packages/source/n/numpy/numpy-1.9.2.tar.gz
rm -r numpy-1.9.2
tar -zxvf numpy-1.9.2.tar.gz
cd numpy-1.9.2

sudo python setup.py install

sudo apt-get -y install python-nose
python -c "import numpy as np; np.test()"


