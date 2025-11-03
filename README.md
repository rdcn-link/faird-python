faird refactored version (2025-05-07)

## Environment Configuration

### 1.Install conda

**1.1 Download Miniconda (Python 3 version, refer to this link for reference) **

```wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh```

**1.2 Install Miniconda**

```bash Miniconda3-latest-Linux-x86_64.sh```

**1.3  Configure conda domestic mirror (University of Science and Technology of China’s mirror is recommended) **

```conda config --add channels https://mirrors.ustc.edu.cn/anaconda/pkgs/main/
conda config --add channels https://mirrors.ustc.edu.cn/anaconda/pkgs/free/
conda config --add channels https://mirrors.ustc.edu.cn/anaconda/cloud/conda-forge/
conda config --set show_channel_urls yes
```

### 2.Create a Python virtual environment

**2.1 Creation command (Available Python versions for pyarrow 19.0.0: 3.9, 3.10, 3.11, 3.12 and 3.13) **

```conda create --name py312 python=3.12.0```

**2.2 Activate the environment **

```conda activate py312```

**2.3 Install dependencies **

```conda install --file requirements.txt```


### 3.Start the service

```python app/main.py```
