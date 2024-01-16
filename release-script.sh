#!/bin/bash

# release all individual platforms
script_location = ./src/framework/processing/py/port/script.py

all_platforms='platforms = \[ ("LinkedIn", extract_linkedin, linkedin.validate), ("Insta'
all_platforms_commented_out='#platforms = \[ ("LinkedIn", extract_linkedin, linkedin.validate), ("Insta'

# comment out all platforms in sequence
sed -i "s/$all_platforms/$all_platforms_commented_out/g" $script_location
