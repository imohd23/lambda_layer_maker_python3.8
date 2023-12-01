import json
import sys
import os
import shutil
import boto3
from subprocess import run
import urllib.request
from zipfile import ZipFile


def lambda_handler(event, context):
    # Declare boto3 resouces
    S3 = boto3.resource("s3")
    Bucket = S3.Bucket(event["s3_bucket"])
    Lambda = boto3.client("lambda")

    # Define required values in event
    required_fields = ["layer_name", "s3_bucket", "libraries", "action"]
    # Pass the dict into the validation process
    validate = validate_field(event, required_fields)
    if not validate and len(validate) > 1:
        return validate

    # Test Access permision to S3
    try:
        Bucket.objects.limit(count=1)
    except Exception:
        return "This function has no access to this bucker or its not exists, please validate"

    # Test access to lambda resources
    try:
        Lambda.list_functions()
    except Exception:
        return "This function has no access to Lambda resources, please validate"

    if event["action"] == "create_new":
        # Create a new layer
        action_return = create_new(event, Bucket, Lambda)
        return {"Layer ARN: ": action_return}
    elif event["action"] == "read_only":
        # List all installed libraries and its versions
        action_return = read_only(event, Bucket, Lambda)
        return {"Current Layer": action_return}
    elif event["action"] == "update":
        action_return = update(event, Bucket, Lambda)
        return {"New Layer ARN": action_return}


def update(event, Bucket, Lambda):
    """
    The `update` function updates a Lambda layer by downloading the current layer content, installing
    specified libraries, checking the size limit, zipping the installed libraries, uploading the zip
    file to S3, creating a new layer version, and returning the ARN of the new layer version.

    :param event: The `event` parameter is a dictionary that contains the following keys:
    :param Bucket: The "Bucket" parameter refers to the name of the S3 bucket where the Python.zip file
    will be uploaded
    :param Lambda: The `Lambda` parameter is an object or client that allows you to interact with AWS
    Lambda service. It provides methods to perform operations such as listing layer versions, getting
    layer version details, publishing layer versions, etc
    :return: the ARN (Amazon Resource Name) of the newly created layer version.
    """
    # Getting the layer version
    list_layer_version = Lambda.list_layer_versions(LayerName=event["layer_name"])
    layer_version = list_layer_version["LayerVersions"][0]["Version"]
    # getting the layer version details
    get_layer_details = Lambda.get_layer_version(
        LayerName=event["layer_name"], VersionNumber=layer_version
    )
    layer_s3_link = get_layer_details["Content"]["Location"]
    # Download nad unzip the layer content
    urllib.request.urlretrieve(layer_s3_link, "/tmp/python.zip")
    shutil.unpack_archive("/tmp/python.zip", "/tmp", "zip")
    # Update the current libraries
    for library in event["libraries"]:
        run(["python", "-m", "pip", "install", library, "-t", "/tmp/python"])
    # Calculate layer limit
    dir_size = run(["du", "-sh", "/tmp/python"], capture_output=True, text=True)
    dir_size = dir_size.stdout.split()[0]
    dir_size = dir_size.split("M")[0]
    if int(dir_size) >= 250:
        return (
            "Layer size is over limit, please consider removing unnecessary libraries"
        )
    # Zip the installed libraries
    zip_directory("/tmp/python/", "/tmp/python.zip")
    # Upload the library into S3
    try:
        Bucket.upload_file("/tmp/python.zip", "python.zip")
    except Exception as e:
        return {"ERROR: ": e}
    # Create a new layer
    try:
        new_layer = Lambda.publish_layer_version(
            LayerName=event["layer_name"],
            Content={"S3Bucket": event["s3_bucket"], "S3Key": "python.zip"},
            CompatibleRuntimes=["python3.11"],
            CompatibleArchitectures=["x86_64", "arm64"],
        )
    except Exception as e:
        print(e)
    # Return layer version ARN
    return new_layer["LayerVersionArn"]


def read_only(event, Bucket, Lambda):
    """
    The `read_only` function retrieves the versions of Python libraries installed in a specified Lambda
    layer.

    :param event: The `event` parameter is a dictionary that contains the necessary information for the
    function to execute. It should have a key called "layer_name" which represents the name of the layer
    :param Bucket: The "Bucket" parameter is the name of the S3 bucket where the layer content is stored
    :param Lambda: The `Lambda` parameter is an object or instance of a class that provides methods for
    interacting with AWS Lambda functions. It is used to call methods such as `list_layer_versions` and
    `get_layer_version` to retrieve information about the Lambda layer
    :return: a dictionary `libraries_json` which contains the names of libraries as keys and their
    corresponding versions as values.
    """
    # Prepare the dict
    libraries_json = {}
    # Getting the layer version
    list_layer_version = Lambda.list_layer_versions(LayerName=event["layer_name"])
    layer_version = list_layer_version["LayerVersions"][0]["Version"]
    # getting the layer version details
    get_layer_details = Lambda.get_layer_version(
        LayerName=event["layer_name"], VersionNumber=layer_version
    )
    layer_s3_link = get_layer_details["Content"]["Location"]
    # Download nad unzip the layer content
    urllib.request.urlretrieve(layer_s3_link, "/tmp/python.zip")
    shutil.unpack_archive("/tmp/python.zip", "/tmp", "zip")
    # Get the libraries details
    libraries_list = run(
        ["pip", "list", "--path", "/tmp/python"], capture_output=True, text=True
    )
    libraries_list = libraries_list.stdout.split()[4:]
    # Looping over the return and convert it into dict
    count = 0
    while count != len(libraries_list):
        count_val = count + 1
        libraries_json[libraries_list[count]] = libraries_list[count_val]
        count = count_val + 1
    # Return libraries versions
    return libraries_json


def create_new(event, Bucket, Lambda):
    """
    The `create_new` function creates a new layer in AWS Lambda with specified libraries and returns the
    ARN of the new layer version.

    :param event: The `event` parameter is a dictionary that contains the following information:
    :param Bucket: The "Bucket" parameter refers to the name of the S3 bucket where the Python library
    zip file will be uploaded
    :param Lambda: The "Lambda" parameter refers to an AWS Lambda service object or client that is used
    to interact with AWS Lambda functions. It is used to perform operations such as listing layer
    versions, publishing layer versions, and other Lambda-related tasks
    :return: either "A layer with this name already exists." if a layer with the same name already
    exists, or the ARN (Amazon Resource Name) of the newly created layer version.
    """
    # Check if the layer already exists
    try:
        list_layer_version = Lambda.list_layer_versions(LayerName=event["layer_name"])
        list_layer_version["LayerVersions"][0]["Version"]
    except Exception:
        pass
    else:
        return "A layer with this name already exists."

    # Make sure the dir is empty
    run(["rm", "-rf", "/tmp/*"])
    # Prepare the dir
    run(["mkdir", "/tmp/python"])
    # Install the new libraries
    for library in event["libraries"]:
        run(["python", "-m", "pip", "install", library, "-t", "/tmp/python"])

    # Calculate layer limit
    dir_size = run(["du", "-sh", "/tmp/python"], capture_output=True, text=True)
    dir_size = dir_size.stdout.split()[0]
    dir_size = dir_size.split("M")[0]
    dir_size = dir_size.split("K")[0]
    if int(dir_size) >= 250:
        return (
            "Layer size is over limit, please consider removing unnecessary libraries"
        )
    # Zip the installed libraries
    zip_directory("/tmp/python/", "/tmp/python.zip")
    # Upload the library into S3
    try:
        Bucket.upload_file("/tmp/python.zip", "python.zip")
    except Exception as e:
        return {"ERROR: ": e}
    # Create a new layer
    try:
        new_layer = Lambda.publish_layer_version(
            LayerName=event["layer_name"],
            Content={"S3Bucket": event["s3_bucket"], "S3Key": "python.zip"},
            CompatibleRuntimes=["python3.11"],
            CompatibleArchitectures=["x86_64", "arm64"],
        )
    except Exception as e:
        print(e)
    # Return layer version ARN
    return new_layer["LayerVersionArn"]


def validate_field(event, required):
    """
    The function `validate_field` checks if required keys are missing from the event dictionary and if
    their corresponding values are empty or missing.

    :param event: The `event` parameter is a dictionary that contains the keys and values to be
    validated. It represents the data that needs to be checked for missing keys or empty values
    :param required: The `required` parameter is a list of keys that are required in the `event`
    dictionary
    :return: a string that contains the validation results. If there are missing keys, the string will
    include "Missing Keys: " followed by the list of missing keys. If there are missing values, the
    string will include "*** Missing values: " followed by the list of keys with missing values. If
    there are no missing keys or values, the string will be empty.
    """
    # Specify the variables to gather the validation
    fields = ""
    keys = ""
    values = ""

    # Check if the keys are passed into the function
    keys = [i for i in required if i not in [key for (key, val) in event.items()]]
    # Check if the values are passed into the function
    values = [key for (key, val) in event.items() if not val or len(val) <= 0]

    # Return the validation results
    if keys:
        fields = "Missing Keys: " + str(keys)
    if values:
        fields = fields + "*** Missing values: " + str(values)
    return fields


def zip_directory(folder_path, zip_path):
    """
    The function `zip_directory` takes a folder path and a zip path as input, and creates a zip file
    containing all the files in the specified folder.

    :param folder_path: The folder path is the path to the directory that you want to zip. It should be
    a string representing the absolute or relative path to the folder
    :param zip_path: The `zip_path` parameter is the path where you want to save the zip file. It should
    include the file name and the extension. For example, if you want to save the zip file as
    "my_folder.zip" in the current directory, you can set `zip_path` as "my
    """
    with ZipFile(zip_path, mode="w") as zipf:
        len_dir_path = len(folder_path)
        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                zipf.write(file_path, "python/" + file_path[len_dir_path:])
