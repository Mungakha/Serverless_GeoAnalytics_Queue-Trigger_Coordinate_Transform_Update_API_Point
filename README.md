# Serverless_GeoAnalytics_Queue-Trigger_Coordinate_Transform_Update_API_Point

The queue storage trigger runs a function as messages are added to Azure Queue storage.

Azure Queue Storage is a service for storing large numbers of messages.

It is a nested service in any Azure Storage Account.Under each Storage Account, you can have one or multiple queues.

Each queue in Azure Storage is a container of messages and can store upto 500tb of data

This function has two queues in use. Check the function bindings in the function.json file

"outqueue3" is the trigger queue. When it receives a message, it starts the function."outqueue4" is the output queue, the functions deposits the very message that triggered it once it has successfully run. 

Serverless_GeoAnalytics_Queue-Trigger1 function has been embedded with a python code running of multiple libraries.

requirements.txt file Contains the list of Python packages used to run the python code integrated in the function.These packages on requirements.txt are automatically installed in visual studio code when running the function locally and on Azure when the function is published in Azure.

Specifically, this function logs onto ESRI Portal, downloads a feature service with a defined feature ID, dissolves feature service on specific columns and computes the polygon centroids. What each line or a collection of lines do is detailed in the code.

local.settings.json file contains app settings and connection strings used when running locally. This file doesn't get published to Azure but options are given to have it uploaded when publishing Function.

Notable in this function's local.settings.json file is the integration of Key Vault ID which provide secure access to passwords stored in the vault ensuring they are not embedded in the code.
