ClusterClone EntityRegistration CanvasApp
==============================

This is a template repo to build Canvas apps in Benchling. It contains the data analysis workflows for the Entity Registration in the ClusterClone (Biofoundry) project.



## Table of Contents

1. [Description](#description)  
2. [Prerequisites](#prerequisites)
3. [Setup](#setup)  
4. [Environment Variables](#environment-variables)  
5. [Usage](#usage)  
   - [Running the Test Stack](#running-the-test-stack)  
   - [Running the Deployment Stack](#running-the-deployment-stack)   <!--6. [Cleanup](#cleanup) -->  
6. [Project Organization](#project-structure)  




# Description

This repository contains:

- A Flask‐based Benchling Canvas application served by Gunicorn  
- An Nginx reverse proxy setup (using a predefined server domain) for production deployments
- A Cloudflare tunnel for secure testing without exposing ports directly
- Two separate Docker Compose files: one for testing and one for deployment:
  1. **`docker-compose-test.yaml`** — for local/testing environments  
  2. **`docker-compose.yaml`** — for production/deployment on the VM  
- Python code to:
  - Register entities 
  - Fill up plates
  - Read and create csv files registered in benchling 
  - Find and fill result tables in a notebook

If you want to learn more about how to build Canvas Apps in Benchling or what other cool things can be applied, log in the Benchling community and check this resources: 
- [App Canvas | Building an App Workshop](https://community.benchling.com/workshop-content-50/app-canvas-building-an-app-workshop-1091) - there are 2 videos of >1h each, good guided tour of all the files and how are they connected, using in the repo [app-examples-python](https://github.com/benchling/app-examples-python/tree/main/examples/chem-sync-local-flask). 
- [App Canvas Deep Dive](https://community.benchling.com/workshop-content-50/app-canvas-deep-dive-1280)  - 1 video of >1h, perfect to learn more about Canvas you’ve already had a chance to play around with it


# Prerequisites

- Docker (≥ 20.10) and Docker Compose (embedded or plugin) installed on your machine  
- An `.env` file at the project root with all required environment variables (see below)  
- (For production) A domain pointing to your VM and the proper firewall rules.  




# Setup

1. Generate a new repo from this template
   - On GitHub, go to Use this template → Create a new repository 
   - Name it (e.g. `benchling-canvas-foo`) and click Create repository from template

2. Clone your newly created repo 
   ```bash
   git clone git@github.com:<your-org>/<your-new-repo>.git benchling-canvas
   cd benchling-canvas
    ```

3. Make your local copy of the files named `.env.template` and `.client_secret.txt.template`, and add their local names into `.gitignore` (if not already listed) :
    ```bash
    cp .env.template .env
    cp .client_secret.txt.template .client_secret.txt
    ```

<!--  3. Be sure to place your local `.env` and `client_secret.txt` file in the same folder (or update the path) so that Docker can mount it as a secret.
--> 
4. Create & activate a Python virtual environment
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```
5. Install dependencies
    ```bash
    pip install --upgrade pip setuptools wheel
    pip install -r requirements-full.txt
    ```


# Environment Variables

Make sure your local `.env` file includes:

- `CLIENT_ID` – Benchling OAuth client ID  
- `APP_DEFINITION_ID` – Benchling app definition ID  
- `CLIENT_SECRET_FILE` – Path to the secrets file (e.g. `/run/secrets/app_client_secret`)  
<!--- `BENCHLING_APP_LOG_LEVEL` – (e.g. `DEBUG` or `INFO`)   -->  
- Any other variables your Flask app or Nginx configuration requires  

Once you have created the app from manifest in Bencling, you'll be able to copy the client secret in `client_secret.txt`.
You can find a better explanation on the original Benchling repo on Canvas apps: [app-examples-python](https://github.com/benchling/app-examples-python/tree/main/examples/chem-sync-local-flask)



# Usage

### Create the Canvas app in Benchling

Create an app from Benchling's developer consol from a manifest, using this repo's manifest as a template. Change the name of your app in info and features. If you need to access specific folders or entity schemas in your code (maybe through Benchling SDK), you should define them here so you don't have problems of access when running the app. The project needs to be defined (maybe also the registry). The rest of the folders and entities depend on your code. 

If you have updated the manifest you can change it in Version history (in the developer consol). You will have to define the different folders and entities schemas before running the app for the first time. In the developer consol, inside of the app there is a button in the top right corner ```View app in the workspace```. You can select the right values for each schema under the tab Configuration

1. **Generate a New App from Manifest**  
   - In Benchling’s Developer Console, click **Create App** and choose **From Manifest**.  
   - Use this repository’s `manifest.yaml` as your template.  
   - Update the app’s name and id in **info**, and **features** fields to match your requirements.

2. **Define Required Permissions**  
   - If your code needs to access specific folders or entity schemas (e.g., via the Benchling SDK), add those permissions under **Configuration** in the manifest.  
   - Ensure you include any **Project** or **Registry** definitions your app will use. Missing these will cause access errors at runtime.  
   - Leave folders and schemas blank, you will select their values from Benchling (this also allows easier implementation in both testing and deployment environments at the same time).

3. **Update the Manifest Version (Optional)**  
   - If you make changes to `manifest.yaml`, go to **Version History** in the Developer Console and upload your updated manifest.  

4. **Configure Schemas Before First Run**  
   - Inside your app’s settings in the Developer Console , click **View App in the Workspace** (top right corner).  
   - Under the **Configuration** tab, select the correct values for each folder and entity schema you defined.  
   - Save your configuration—this ensures that when you run the app, it has permission to read/write the appropriate Benchling data, and you can have different values selected in different environments.

5. **Run docker desktop**

6. **Grant extra access for testing environment**  
   - You may need to grant extra access in the Benchling test environment. In Benchling, open the target project, go to Project Settings → Collaborators, add your app as a collaborator, and assign the Read access policy.
   - This is usually not required in Benchling Deployment environment if the app is installed with the right scopes (the information in the Manifest).
   - In any case: Make sure all required entity schemas and folders you are using though the app are declared in the manifest so the app can read what it needs.

7. **Run the correct Stack**  

    #### Running the Test Stack

    This is intended for local development, using `docker-compose-test.yaml`.<!--   It may enable debugging flags, bind ports differently, or skip SSL.--> 
    Run this code in the terminal:

    ```bash
    docker compose -f docker-compose-test.yaml --env-file .env up --build
    ```
    ```bash
    docker compose \
      -f docker-compose-test.yaml \
      --env-file .env \
      up --build
    ```
    **Configuring Your Webhook URL**

    When testing via Cloudflare, copy the HTTPS URL provided by the tunnel and update the Webhook URL in your Benchling App to use that exact address. Be sure to append ```/1/webhooks``` to the end of the HTTPS URL so that Benchling can properly deliver events.
    After loading a new URL you should test that it works. You can do that from the same developer consol in Benchling, but in your app's tab called webhook testing. There you should select ```v2.canvas.initialized``` under Preview and Message and then click send. If it's a success you can start using the app

    #### Running the Deployment Stack

    Use this on your VM (or any production‐like environment). It assumes your domain is already pointed at this machine, and that ports 80/443 are open.

    ```bash 
    docker compose \
      -f docker-compose.yaml \
      --env-file .env \
      up --build -d
      ```

8. **Webhook testing**  
   - Whenever you change the app URL (e.g., switching between deploy and test Docker Compose), verify that Benchling can reach your webhook:
    1. In Benchling, open Developer Console and select your app.
    2. Go to the Webhook testing tab (after changing the URL).
    3. Choose a message type (e.g., canvas.created or canvas.initialized).
    4. Click Send test.
   - If you get 200 OK, the URL and webhook endpoint are working—now you can run the app in a notebook.

### Nginx

**What is Nginx and how do we use it?**

Nginx is a high-performance web server that also works as a reverse proxy and TLS (HTTPS) terminator. In the deployment stack in our setup it:
- Exposes one public HTTPS URL on the VM (port 443) and routes requests to the correct Docker app based on the path (e.g., /1/webhooks → App A, /2/webhooks → App B).
- Handles SSL certificates (serves the cert/key) so the apps can stay simple and only speak HTTP inside the Docker network.
- Lets multiple Canvas apps share the same VM and domain safely and cleanly.

**Certificates**

- Use trusted certs (e.g., Let’s Encrypt) and renew them periodically (e.g., via certbot). If you use Let’s Encrypt + certbot (dockerized or on host), renew into /etc/letsencrypt/... or /opt/cert/..., then reload Nginx:
```bash
docker compose exec nginx nginx -s reload
```
- !! Do not commit private keys to Git. Keep cert/key paths mounted read-only into the Nginx container.!!

**Important gotchas**

- Path and code must match: If Benchling routes /1/webhooks/, nginx and your app (app.py) must listen on that path.
- If you only have one app in the VM it can have its own Nginx, but a top-level Ngix container can be developed to manage and route different webhooks (e.g., /1/webhooks → App A, /2/webhooks → App B).
- Firewall/NSG: Ensure port 443 is open to the world and ports 5000 are not exposed on the host (only inside Docker network).
- Again, Don’t commit secrets: Never commit domain.key or any private key to Git.



<!-- 
## Cleanup

To stop and remove all containers, networks, and volumes created by the chosen Compose file:

# If you used test compose:
docker compose -f docker-compose.test.yml down

# If you used deploy compose:
docker compose -f docker-compose.deploy.yml down 
 

--> 

# Project Organization
------------
    ├── LICENSE
    ├── README.md                 <- The top-level README for developers using this project.
    ├── TODO.md                   <- A helpful TODO list for project related tasks.
    ├── .env                      <- contains usernames/passwords to access database, etc (file is ignored with .gitignore)
    ├── .env_template             <- template to make your own .env file
    ├── certificates              <- eg. ssl certificate needed for sql access (contents ignored with .gitignore)
    ├── data
    │   ├── external              <- Data from third party sources.
    │   ├── interim               <- Intermediate data that has been transformed.
    │   ├── processed             <- The final, canonical data sets for modeling.
    │   └── raw                   <- The original, immutable data dump.
    │
    ├── notebooks                 <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                                and a short `_` delimited description, e.g.
    │                                `01_initial_data_exploration,ipynb`.
    │
    ├── reports                   <- Generated analysis as HTML, PDF, LaTeX etc.
    │   ├── figures               <- Generated graphics and figures to be used in reporting
    │   └── presentations         <- Any slidedecks used for project meetings can go here
    │
    ├── environment.yml           <- The requirements file for reproducing the analysis environment, e.g.
    │                                generated with `conda env export --no-builds -n clusterclone > environment.yml`.
    ├── environment_simple.yml    <- A simpler version of the environment with only the packages directly 
    │                                installed. Ideally generated through `conda env export --from-history -f environment_simple.yml`.
    │
    ├── src                       <- Source code for use in this project.
    │   ├── __init__.py           <- Makes src a Python module
    │   ├── data                  <- Scripts to download or generate data
    │   ├── utilities             <- Scripts or modules for miscellaneous functions
    │   └── visualization         <- Scripts to create exploratory and results oriented visualizations
    │
    └── Anything else?


# Contributing

Some guidelines should be observed consistently across all that contribute to this project, so that this simplified version of a data science structure is still viable.

* Make sure your notebook works in the repository. 

    * If you refer to files in notebooks/scripts, they have to exist in the repository - and not just your own computer. Ideally use the dotenv path system to refer to files in the repository. 
    * Your scripts will need certain libraries to run. Make sure they are listed in the `environment_simple.yml` file. Some requirements are tricky to resolve due to version conflicts. A full environment export can be put on `environment.yml`. Make sure to built up on the previously established environment and/or test/update the rest of the notebooks/scripts accordingly.

* Keep the formatting standard.
    * Use the black formatter to format your code.

* Keep data in the structure presented.
    * Raw data should always be traceable to sources.
    * Interim data need not be well-tracked.
    * Processed data should be easily regeneratable from raw data through scripts or notebooks.

# Sources of external files:

Any data processed/generated outside this repository (and prone to change) should go under `data/external`. When new data file is added to external, the source should be catalogued here.


# Requirements:

TODO: A core environment needs to be established. Alternates are fine but one environment would best if possible. Also the requirements file we have now will probably not be sufficient for some of the code.

`environment_simple.yml` file lists all packages used/needed in scripts and notebooks. This environment can be created by conda using: 

```
conda env create -f environment_simple.yml 
```


<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
