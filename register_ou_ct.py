import boto3
import sys
import json
import time
from botocore.exceptions import ClientError

def get_baseline_identifier(client):
    response = client.list_baselines()
    for baseline in response['baselines']:
        if baseline['name'] == 'AWSControlTowerBaseline':
            return baseline['arn']
    raise Exception("AWSControlTowerBaseline not found")

def get_identity_center_enabled_baseline_arn(client):
    response = client.list_enabled_baselines()
    for baseline in response['enabledBaselines']:
        if 'arn' in baseline['arn']:
            return baseline['arn']
    raise Exception("IdentityCenterBaseline not found")

def enable_baseline(client, ou_id, ou_arn, baseline_identifier, identity_center_arn):
    try:
        response = client.enable_baseline(
            baselineIdentifier=baseline_identifier,
            baselineVersion='4.0',
            targetIdentifier=ou_arn,
            parameters=[
                {
                    'key': 'IdentityCenterEnabledBaselineArn',
                    'value': identity_center_arn
                }
            ]
        )
        print(f"Enable baseline response: {json.dumps(response, default=str)}")
        return response.get('operationIdentifier')
    except ClientError as e:
        print(f"ClientError in enable_baseline: {str(e)}")
        if e.response['Error']['Code'] == 'ConflictException':
            if 'already governed' in e.response['Error']['Message']:
                print(f"OU {ou_id} is already registered with Control Tower")
                return None
            elif 'another operation is in progress' in e.response['Error']['Message']:
                print(f"Another operation is in progress for OU {ou_id}. Waiting...")
                return 'IN_PROGRESS'
        raise

def check_operation_status(client, operation_id):
    max_attempts = 40
    for attempt in range(max_attempts):
        try:
            response = client.get_baseline_operation(operationIdentifier=operation_id)
            msg = f"Baseline operation response: {json.dumps(response, default=str)}"
            print(msg)
            status = response.get('baselineOperation', {}).get('status')
            if status:
                print(f"Operation status: {status}")
                if status == 'SUCCEEDED':
                    return True
                elif status == 'FAILED':
                    return False
            else:
                print(f"Status not found in response: {response}")
            time.sleep(30)
        except ClientError as e:
            print(f"Error checking operation status: {str(e)}")
            time.sleep(30)
    return False

def wait_for_in_progress_operations(
        client, ou_id, ou_arn, baseline_identifier, identity_center_arn
):
    max_attempts = 20
    for attempt in range(max_attempts):
        try:
            operation_id = enable_baseline(
                client, ou_id, ou_arn, baseline_identifier, identity_center_arn
            )
            if operation_id and operation_id != 'IN_PROGRESS':
                return operation_id
            elif operation_id is None:
                return None  # OU is already registered
            time.sleep(60)
        except Exception as e:
            print(f"Error while waiting for in-progress operations: {str(e)}")
            time.sleep(60)
    raise Exception(f"Timed out waiting for in-progress operations for OU {ou_id}")

def register_ou(client, ou_id, ou_arn, baseline_identifier, identity_center_arn):
    print(f"Registering OU: {ou_id}")
    try:
        operation_id = wait_for_in_progress_operations(
            client, ou_id, ou_arn, baseline_identifier, identity_center_arn
        )
        if operation_id:
            if check_operation_status(client, operation_id):
                print(f"Successfully registered OU: {ou_id}")
                return True
            else:
                print(f"Failed to register OU: {ou_id}")
                return False
        else:
            print(f"Skipped OU: {ou_id} (already registered)")
            return True
    except Exception as e:
        print(f"Error registering OU {ou_id}: {str(e)}")
        return False

def register_ous(region, ous):
    client = boto3.client('controltower', region_name=region)

    try:
        baseline_identifier = get_baseline_identifier(client)
        identity_center_arn = get_identity_center_enabled_baseline_arn(client)
    except Exception as e:
        print(f"Error getting baseline information: {str(e)}")
        sys.exit(1)

    for ou in ous:
        ou_id = ou['id']
        ou_arn = ou['arn']

        success = register_ou(
            client, ou_id, ou_arn, baseline_identifier, identity_center_arn
        )
        if not success:
            print(f"Failed to register OU: {ou_id}. Stopping the process.")
            sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python register_ou_control_tower.py <region> <ous_json>")
        sys.exit(1)

    region = sys.argv[1]
    ous_json = sys.argv[2]
    ous = json.loads(ous_json)
    register_ous(region, ous)
