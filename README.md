# NanoEdge™ AI Studio Data Logs Tools

This repository contains scripts and tools to help creating data logs to be used with NanoEdge™ AI Studio.

## Formatting data logs

To format data logs, please use the script `neai_format.py`.
See [documentation](https://cartesiam-neai-docs.readthedocs-hosted.com/studio/studio.html#ii-general-approach-for-formatting-input-files-properly) for how input files should be formatted.

What the script can do:
- Build buffer of data
- Downsample data
- Change value and decimal delimiters
- Remove headers

Usage: 
- Configure parameters inside `neai_format.py` script.
- Run `python3 neai_format.py -i <input_file.csv> -o <output_file.csv>`

Note: `neai_format.py` runs with Python 3 and use only standard libraries.
