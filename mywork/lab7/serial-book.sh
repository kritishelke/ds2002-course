#!/bin/bash
#SBATCH --account=ds2002
#SBATCH --job-name=serial-book
#SBATCH --output=serial-book-%j.out
#SBATCH --error=serial-book-%j.err
#SBATCH --time=00:10:00
#SBATCH --partition=standard
#SBATCH --mem=8G
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1

module load miniforge
source activate ds2002

python ~/ds2002-course/labs/07-hpc/process-book.py \
/scratch/$USER/ds2002-jobruns/text-analysis/book-1.txt \
/scratch/$USER/ds2002-jobruns/text-analysis/results-1.csv
