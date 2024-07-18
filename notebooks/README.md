## Setup

Prior to running a notebook, you should create a new environment and install the required packages. You can do this by running the following commands:

1. Create a new environment
2. Activate the environment
3. Install the required packages

### Environment setup 

#### Conda environments

```bash
# Create a new conda environment
conda create --name <env_name> python=3.11
conda activate <env_name>
```
#### Virtual environments

```bash

# Create a new virtual environment
python3 -m venv <env_name>
source <env_name>/bin/activate
```



### Install required packages

```bash
# Install the required packages
pip install --upgrade pip
pip install -r requirements.txt # or whatever other requirements file you have
```
