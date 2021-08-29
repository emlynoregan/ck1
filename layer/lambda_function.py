import boto3
import botocore
# import jsonschema
import json
import traceback

from extutil import remove_none_attributes, gen_log, creturn, account_context

# {
#     "prev_state": prev_state,
#     "component_def": component_def,
#     "component_name": component_name,
#     "op": op,
#     "s3_object_name": object_name,
#     "pass_back_data": pass_back_data,
#     "bucket": bucket,
#     "repo_id": repo_id,
#     "project_code": project_code
# }

ALLOWED_RUNTIMES = ["python3.8", "python3.6", "python3.7", "nodejs14.x", "nodejs12.x", "nodejs10.x", "ruby2.7", "ruby2.5"]

def lambda_handler(event, context):
    try:
        print(event)
        account_number = account_context(context)['number']
        #Really should be getting region from "default region"
        region = account_context(context)['region']
        logs = []
        prev_state = event.get("prev_state")
        project_code = event.get("project_code")

        cdef = event.get("component_def")
        cname = event.get("component_name")

        if event.get("op") == "upsert":
            bucket = event.get("bucket")
            object_name = event.get("s3_object_name")

            layer_name = cdef.get("layer_name") or f"{project_code}_{cname}"

            description = cdef.get("description") or f"Layer for component {cname} and project {project_code}"

            compatible_runtimes = cdef.get("compatible_runtimes")
            if not compatible_runtimes:
                return creturn(200, 0, error=f"you must provide compatible_runtimes")
            if not isinstance(compatible_runtimes, list):
                return creturn(200, 0, error=f"compatible_runtimes must be a list of strings")
            
            pass_back_data = event.get("pass_back_data", {})

            desired_config = remove_none_attributes({
                "LayerName": layer_name,
                "Description": description,
                "Content": {
                    "S3Bucket": bucket,
                    "S3Key": object_name
                },
                "CompatibleRuntimes": compatible_runtimes,
            })

            # if pass_back_data:
            #     ops = pass_back_data.get("ops")
            # elif:
            ops = {"publish_layer_version": True}

            # elif prev_state:
            #     ops = {"get_lambda": True}
            # else:
            #     ops = {"create_function":function_name}

            # if ops.get("get_lambda"):
            #     retval = get_function(prev_state, function_name, desired_config, logs, ops)
            #     print(f"get_function retval = {retval}")
            #     if retval.get("statusCode"):
            #         return retval
            #     logs = retval.pop("logs")
            #     ops = retval.get("ops")

            props = None

            # if ops.get("publish_layer_version"):
            retval = publish_layer_version(desired_config, logs, ops)
            logs = retval.pop("logs")
            ops = retval.get("ops")
            state = retval.get("state")

            props = {
                "layer_name": layer_name,
                **remove_none_attributes(state)
            } if state else {}

            return creturn(200, 100, success=True, logs=logs, 
                state=state, 
                props=props,
                links={
                    "Layer": gen_layer_link(layer_name, region)
                }
            )
            

        elif event.get("op") == "delete":
            layer_name = prev_state['props'].get("layer_name")
            ops = {"remove_layer": layer_name}
            retval = remove_layer(function_name, logs, ops)
            logs = retval.pop("logs")
            return creturn(200, 100, success=True, logs=logs)

    except Exception as e:
        msg = traceback.format_exc()
        print(msg)
        return creturn(200, 0, logs=logs, error=msg)

def publish_layer_version(desired_config, logs, ops):
    lambda_client = boto3.client("lambda")
    print(f"Inside publish_layer_version, desired_config = {desired_config}")

    try:
        lambda_response = lambda_client.publish_layer_version(
            **desired_config
        )
        logs.append(gen_log("Published Layer Version", lambda_response))

    except botocore.exceptions.ClientError as e:
        logs.append(gen_log(e.response["Error"]["Code"], {"error": str(e)}, is_error=True))
        msg = traceback.format_exc()
        print(msg)
        return creturn(400, 60, logs=logs, error = msg)

        # if e.response['Error']['Code'] in ['PreconditionFailed', 'CodeVerificationFailed', 'InvalidCodeSignature', 'CodeSigningConfigNotFound']:
        #     return creturn(200, 60, logs=logs, error = str(e))
        # else:
        #     print(f'Reached other exceptions, exception is {str(e)}')
        #     return creturn(200, 60, pass_back_data={
        #         "ops": ops,
        #     }, logs=logs)

    _ = ops.pop("publish_layer_version")

    return {"ops": ops, "logs": logs, "state": lambda_response}

def remove_layer(layer_name, logs, ops):
    lambda_client = boto3.client("lambda")

    try:
        first = True
        layer_versions = []
        marker=None
        while first or layer_versions:
            first = False

            for layer_version in layer_versions:
                delete_retval = lambda_client.delete_layer_version(LayerName=layer_name, VersionNumber = layer_version.get("VersionNumber"))
                logs.append(gen_log(f"Deleted layer version", layer_version))

            layer_versions_retval = lambda_client.list_layer_versions(
                **remove_none_attributes({
                    "LayerName": layer_name,
                    "Marker": marker
                })
            )            
            logs.append(gen_log(f"Listed layer versions", layer_versions_retval))

            marker = layer_versions_retval.get("NextMarker")
            layer_versions = layer_versions_retval.get("LayerVersions")


    except botocore.exceptions.ClientError as e:
        msg = traceback.format_exc()
        print(msg)
        return creturn(200, 40, pass_back_data={
            "ops": ops
        }, logs=logs, error=msg)

    _ = ops.pop("remove_layer")

    return {"ops": ops, "logs": logs}

def gen_layer_link(layer_name, region):
    return f"https://console.aws.amazon.com/lambda/home?region={region}#/layers/{layer_name}"



