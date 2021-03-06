#!/bin/tcsh

echo Making features....

# first take the input fasta file and convert it to features suitable for input to the
#        prediction  script
python3 KmerFeatures.py -f $1 -o $1:r_k14red0_features -m simple -M reduced_alphabet_0 -k 14 -F data/best_model_1280features.txt

echo Making predictions...

# next use the features as input to apply the derived model
Rscript --file=SIEVEUbApply.R $1:r_k14red0_features.txt

# this will output predictions as text
