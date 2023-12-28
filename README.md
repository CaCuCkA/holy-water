# Holy Water Test Task

Test task for the Data Engineer position at Holy Water, involving the integration of key technologies, including
BigQuery, Google Scheduler, Google Cloud Functions, and Google Data Studio.

## Prerequisites

❯ Python >= 3.8 \
❯ google-cloud \

## Installation

To install our project, you need to clone the repository first:

```bash
$ mkdir ~/workdir
$ cd ~/workdir
$ git clone https://github.com/CaCuCkA/holy-water.git
$ cd holy-water
```

## Google Cloud Setup Steps:

The code is currently operational on my private Google Cloud server as a Google Cloud Function. Here's my proposed plan:

1. Establish a Google Scheduler task configured to execute the script daily at 3:00 PM, fetching data from the previous
   day.
2. Create a Google Cloud Function and incorporate `main.py` along with `requirements.txt` into its body. Add your Auth
   Key for the Holy Water API as a Google environment variable.

If you're interested in developing a dashboard, I recommend using Google Data Studio.

## Dashboard

I leave reference for my dashboard
also [here](https://lookerstudio.google.com/reporting/d50bb706-7785-4bbf-8ca1-4a5bc024a0a3). 