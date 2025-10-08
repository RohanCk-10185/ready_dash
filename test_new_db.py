#!/usr/bin/env python3
"""
Test script for the new database schema with per-cluster tables
"""

import os
import sys
from datetime import datetime

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import (
    create_db_and_tables, SessionLocal, update_cluster_data, 
    get_all_clusters_summary, get_cluster_details, delete_cluster,
    ClusterRegistry
)

def test_new_db_schema():
    """Test the new database schema"""
    print("Testing new database schema...")
    
    # Create tables
    create_db_and_tables()
    print("✓ Database tables created")
    
    # Create a test session
    session = SessionLocal()
    
    try:
        # Test data for a cluster
        test_cluster_data = {
            'name': 'test-cluster',
            'account_id': '123456789012',
            'region': 'us-west-2',
            'arn': 'arn:aws:eks:us-west-2:123456789012:cluster/test-cluster',
            'version': '1.28',
            'platformVersion': 'eks.1',
            'endpoint': 'https://test-cluster.gr7.us-west-2.eks.amazonaws.com',
            'status': 'ACTIVE',
            'createdAt': datetime.now().isoformat(),
            'roleArn': 'arn:aws:iam::123456789012:role/eks-cluster-role',
            'tags': {'Environment': 'test'},
            'health_issues': None,
            'eks_auto_mode': 'DISABLED',
            'networking': {'vpcId': 'vpc-12345'},
            'oidc_provider_url': 'https://oidc.eks.us-west-2.amazonaws.com/id/ABC123',
            'security_insights': {'status': 'ENABLED'},
            'workloads': {'pods': 10, 'deployments': 5},
            'certificateAuthority': {'data': 'LS0tLS1CRUdJTi...'},
            'nodegroups_data': [
                {
                    'name': 'ng-1',
                    'status': 'ACTIVE',
                    'amiType': 'AL2_x86_64',
                    'instanceTypes': ['t3.medium'],
                    'releaseVersion': '1.28.1-20231201',
                    'version': '1.28',
                    'createdAt': datetime.now().isoformat(),
                    'desiredSize': 2,
                    'is_karpenter_node': False
                }
            ],
            'addons': [
                {
                    'addonName': 'vpc-cni',
                    'addonVersion': 'v1.12.6-eksbuild.2',
                    'status': 'ACTIVE',
                    'pod_identity_display': 'enabled',
                    'irsa_role_arn': 'arn:aws:iam::123456789012:role/eksctl-test-cluster-addon-iamserviceaccount-Role1'
                }
            ],
            'fargate_profiles': [
                {'name': 'fp-1', 'status': 'ACTIVE'}
            ],
            'access_entries': [
                {
                    'principalArn': 'arn:aws:iam::123456789012:user/test-user',
                    'type': 'STANDARD',
                    'username': 'test-user',
                    'groups': ['system:masters'],
                    'access_policies': []
                }
            ]
        }
        
        # Test 1: Create/Update cluster
        print("\n1. Testing cluster creation/update...")
        cluster = update_cluster_data(session, test_cluster_data)
        print(f"✓ Cluster created/updated: {cluster.name} (table: {cluster.table_name})")
        
        # Test 2: Get all clusters summary
        print("\n2. Testing get_all_clusters_summary...")
        clusters_summary = get_all_clusters_summary(session)
        print(f"✓ Found {len(clusters_summary)} clusters")
        for c in clusters_summary:
            print(f"  - {c['name']} ({c['account_id']}/{c['region']})")
        
        # Test 3: Get cluster details
        print("\n3. Testing get_cluster_details...")
        cluster_details = get_cluster_details(session, '123456789012', 'us-west-2', 'test-cluster')
        if cluster_details:
            print(f"✓ Cluster details retrieved: {cluster_details['name']}")
            print(f"  - Nodegroups: {len(cluster_details['nodegroups_data'])}")
            print(f"  - Addons: {len(cluster_details['addons'])}")
            print(f"  - Fargate profiles: {len(cluster_details['fargate_profiles'])}")
            print(f"  - Access entries: {len(cluster_details['access_entries'])}")
        else:
            print("✗ Failed to get cluster details")
        
        # Test 4: Update cluster with new data
        print("\n4. Testing cluster update...")
        test_cluster_data['nodegroups_data'].append({
            'name': 'ng-2',
            'status': 'ACTIVE',
            'amiType': 'AL2_x86_64',
            'instanceTypes': ['t3.large'],
            'releaseVersion': '1.28.1-20231201',
            'version': '1.28',
            'createdAt': datetime.now().isoformat(),
            'desiredSize': 1,
            'is_karpenter_node': False
        })
        
        update_cluster_data(session, test_cluster_data)
        updated_details = get_cluster_details(session, '123456789012', 'us-west-2', 'test-cluster')
        print(f"✓ Cluster updated: {len(updated_details['nodegroups_data'])} nodegroups")
        
        # Test 5: Delete cluster
        print("\n5. Testing cluster deletion...")
        deleted = delete_cluster(session, '123456789012', 'us-west-2', 'test-cluster')
        if deleted:
            print("✓ Cluster and its tables deleted successfully")
        else:
            print("✗ Failed to delete cluster")
        
        # Verify deletion
        remaining_clusters = get_all_clusters_summary(session)
        print(f"✓ Remaining clusters: {len(remaining_clusters)}")
        
        print("\n🎉 All tests passed! New database schema is working correctly.")
        
    except Exception as e:
        print(f"✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()

if __name__ == "__main__":
    test_new_db_schema()
