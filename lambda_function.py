import boto3
import logging
from os import environ
import datetime
from datetime import *
from time import *
import dateutil.tz
from distutils.util import strtobool
regions = ['us-east-1']
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def findTagSchedule(tags):
    schedule = {
        'enabled': strtobool("False"),
        'start_time': '',
        'stop_time': ''
    }
    for tag in tags:
        if tag['Key'] == 'Scheduled':
            schedule['enabled'] = strtobool(tag['Value'])
        elif tag['Key'] == 'start_time':
            schedule['start_time'] = tag['Value']
        elif tag['Key'] == 'stop_time':
            schedule['stop_time'] = tag['Value']
           
    return schedule

def do_schedule_rds(region, hourminute):
    rds = boto3.client('rds', region_name=region)
    instances = rds.describe_db_instances()['DBInstances']
    if instances:
        for instance in instances:
            if (instance['Engine'] != 'docdb'):
                tags = rds.list_tags_for_resource(ResourceName=instance['DBInstanceArn'])
                scheduleTag = findTagSchedule(tags['TagList'])
                if bool(scheduleTag['enabled']):
                    instanceid = instance['DBInstanceIdentifier']
                    print ('RDS DBInstance: ' + instanceid + ' with tag scheduled = true')
                    if scheduleTag['start_time'] and (instance['DBInstanceStatus'] in ['stopped', 'stopping']) and (int(hourminute) <= int(scheduleTag['start_time']) <= (int(hourminute) + 15)):
                        startUp = rds.start_db_instance(DBInstanceIdentifier=instanceid)
                        print(startUp)
                    if scheduleTag['stop_time'] and (instance['DBInstanceStatus'] in ['available', 'starting']) and (int(hourminute) <= int(scheduleTag['stop_time']) <= (int(hourminute) + 15)):
                        shutDown = rds.stop_db_instance(DBInstanceIdentifier=instanceid)
                        print(shutDown)

def do_schedule_ec2(region, hourminute):
    ec2         = boto3.resource('ec2', region_name=region)
    autoscaling = boto3.client('autoscaling', region_name=region)
    paginator = autoscaling.get_paginator('describe_auto_scaling_groups')
    page_iterator = paginator.paginate(
        PaginationConfig={'PageSize': 100}
    )
    filters = [
        {
            'Name': 'tag:Scheduled',
            'Values': ['true']
        }
    ]
    instances = ec2.instances.filter(Filters=filters)
    ec2Instances = [instance.id for instance in instances]
    print ('All instances with tag scheduled = true:')
    print (ec2Instances)
    for instance in instances:
        isASG = False
        for tag in instance.tags:
            if tag['Key'] == 'aws:autoscaling:groupName':
                isASG = True
                break
        if isASG:
            continue
        for tag in instance.tags:
            if tag['Key'] == 'start_time':
                start_time = tag['Value']
                if instance.state['Name'] == 'stopped' and (int(hourminute) <= int(start_time) <= (int(hourminute) + 15)):
                    startUp = instance.start()
                    print(startUp)
                    break
            if tag['Key'] == 'stop_time':
                stop_time = tag['Value']
                if instance.state['Name'] != 'stopped' and (int(hourminute) <= int(stop_time) <= (int(hourminute) + 15)):
                    shutDown = instance.stop()
                    print(shutDown)
                    break
    filtered_asgs = page_iterator.search(
        'AutoScalingGroups[] | [?Tags[?Key==`{}` || Value==`{}`]]'.format(
            'Scheduled', 'true'
        )
    )
    print ('All ASGs with tag scheduled = true:')
    for asg in filtered_asgs:
        print(asg['AutoScalingGroupName'])
        for tag in asg['Tags']:
            if tag['Key'] == 'start_time':
                start_time = tag['Value']
                if (int(hourminute) <= int(start_time) <= (int(hourminute) + 15)):
                    startUp = autoscaling.update_auto_scaling_group(
                        AutoScalingGroupName=asg['AutoScalingGroupName'],
                        MinSize=4,
                        MaxSize=6
                    )
                    print(startUp)
                    break
            if tag['Key'] == 'stop_time':
                stop_time = tag['Value']
                if (int(hourminute) <= int(stop_time) <= (int(hourminute) + 15)):
                    shutDown = autoscaling.update_auto_scaling_group(
                        AutoScalingGroupName=asg['AutoScalingGroupName'],
                        MinSize=4,
                        MaxSize=6
                    )
                    print(shutDown)
                    break

def do_schedule_docdb(region, hourminute):
    docdb = boto3.client('docdb', region_name=region)
    clusters = docdb.describe_db_clusters()['DBClusters']
    if clusters:
        for cluster in clusters:
            if (cluster['Engine'] == 'docdb'):
                tags = docdb.list_tags_for_resource(ResourceName=cluster['DBClusterArn'])
                scheduleTag = findTagSchedule(tags['TagList'])
                if bool(scheduleTag['enabled']):
                    clusterid = cluster['DBClusterIdentifier']
                    print ('DocDB Cluster: ' + clusterid + ' with tag scheduled = true')
                    if scheduleTag['start_time'] and (cluster['Status'] in ['stopped', 'stopping']) and (int(hourminute) <= int(scheduleTag['start_time']) <= (int(hourminute) + 15)):
                        startUp = docdb.start_db_cluster(DBClusterIdentifier=clusterid)
                        print(startUp)
                    if scheduleTag['stop_time'] and (cluster['Status'] in ['available', 'starting']) and (int(hourminute) <= int(scheduleTag['stop_time']) <= (int(hourminute) + 15)):
                        shutDown = docdb.stop_db_cluster(DBClusterIdentifier=clusterid)
                        print(shutDown)

def do_schedule(region):
    america = dateutil.tz.gettz('America/Sao_Paulo')
    hourminute = datetime.now(tz=america)
    hourminute = str(hourminute.strftime("%H%M"))
    do_schedule_rds(region, hourminute)
    do_schedule_ec2(region, hourminute)
    do_schedule_docdb(region, hourminute)

def lambda_handler(event, context):
    for region in regions:
        print('\n###############\nCurrent Region: ' + region + '\n###############\n')
        do_schedule(region)
