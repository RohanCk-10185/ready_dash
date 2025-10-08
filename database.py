import os
import json
import re
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, JSON, ForeignKey, Boolean, Text, MetaData, Table
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import text

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./eks_dashboard.db")

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- Master Clusters Registry ---
class ClusterRegistry(Base):
    __tablename__ = "cluster_registry"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True, nullable=False)
    account_id = Column(String, index=True, nullable=False)
    region = Column(String, index=True, nullable=False)
    arn = Column(String, unique=True)
    version = Column(String)
    platform_version = Column(String)
    endpoint = Column(String)
    status = Column(String)
    created_at = Column(DateTime)
    role_arn = Column(String)
    tags = Column(JSON)
    health_issues = Column(JSON)
    eks_auto_mode = Column(String)
    certificate_authority_data = Column(Text)
    networking = Column(JSON)
    oidc_provider_url = Column(String)
    security_insights = Column(JSON)
    workloads = Column(JSON)
    last_updated = Column(DateTime, default=datetime.utcnow)
    table_name = Column(String, unique=True, nullable=False)  # Name of the cluster's dedicated table

# --- Per-Cluster Table Models (dynamically created) ---
def create_cluster_table_name(account_id: str, region: str, cluster_name: str) -> str:
    """Create a safe table name for a cluster"""
    # Remove special characters and create a safe table name
    safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', f"{account_id}_{region}_{cluster_name}")
    return f"cluster_{safe_name}"

def create_cluster_tables(account_id: str, region: str, cluster_name: str):
    """Create dedicated tables for a cluster"""
    table_name = create_cluster_table_name(account_id, region, cluster_name)
    
    # Define table schemas
    nodegroups_table = Table(
        f"{table_name}_nodegroups",
        Base.metadata,
        Column('id', Integer, primary_key=True, index=True),
        Column('name', String),
        Column('status', String),
        Column('ami_type', String),
        Column('instance_types', JSON),
        Column('release_version', String),
        Column('version', String),
        Column('created_at', DateTime),
        Column('desired_size', Integer),
        Column('is_karpenter_node', Boolean, default=False),
    )
    
    addons_table = Table(
        f"{table_name}_addons",
        Base.metadata,
        Column('id', Integer, primary_key=True, index=True),
        Column('name', String),
        Column('version', String),
        Column('status', String),
        Column('pod_identity_display', String),
        Column('irsa_role_arn', String),
    )
    
    fargate_profiles_table = Table(
        f"{table_name}_fargate_profiles",
        Base.metadata,
        Column('id', Integer, primary_key=True, index=True),
        Column('name', String),
        Column('status', String),
    )
    
    access_entries_table = Table(
        f"{table_name}_access_entries",
        Base.metadata,
        Column('id', Integer, primary_key=True, index=True),
        Column('principal_arn', String, index=True),
        Column('type', String),
        Column('username', String),
        Column('groups', JSON),
        Column('access_policies', JSON),
    )
    
    # Create the tables
    Base.metadata.create_all(bind=engine, tables=[nodegroups_table, addons_table, fargate_profiles_table, access_entries_table])
    
    return table_name

def drop_cluster_tables(account_id: str, region: str, cluster_name: str):
    """Drop dedicated tables for a cluster"""
    table_name = create_cluster_table_name(account_id, region, cluster_name)
    
    with engine.connect() as conn:
        # Drop tables if they exist
        for suffix in ['_nodegroups', '_addons', '_fargate_profiles', '_access_entries']:
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}{suffix}"))
            except Exception as e:
                print(f"Warning: Could not drop table {table_name}{suffix}: {e}")
        conn.commit()

class DataUpdateLog(Base):
    __tablename__ = "data_update_logs"
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    status = Column(String)
    details = Column(Text)


# --- DB Setup & Utils ---
def create_db_and_tables(): 
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

def get_last_update_time(session: Session):
    last_success = session.query(DataUpdateLog).filter(DataUpdateLog.status == 'SUCCESS').order_by(DataUpdateLog.timestamp.desc()).first()
    return last_success.timestamp if last_success else None

def get_cluster_table_name(session: Session, account_id: str, region: str, cluster_name: str) -> str:
    """Get the table name for a cluster from the registry"""
    cluster = session.query(ClusterRegistry).filter(
        ClusterRegistry.account_id == account_id,
        ClusterRegistry.region == region,
        ClusterRegistry.name == cluster_name
    ).first()
    return cluster.table_name if cluster else None

# --- CRUD Operations ---
def update_cluster_data(session: Session, cluster_data: dict):
    account_id = cluster_data.get('account_id')
    region = cluster_data.get('region')
    cluster_name = cluster_data.get('name')
    
    # Check if cluster exists in registry
    cluster = session.query(ClusterRegistry).filter(
        ClusterRegistry.account_id == account_id,
        ClusterRegistry.region == region,
        ClusterRegistry.name == cluster_name
    ).first()

    if not cluster:
        # Create new cluster tables and registry entry
        table_name = create_cluster_tables(account_id, region, cluster_name)
        cluster = ClusterRegistry(
            name=cluster_name,
            account_id=account_id,
            region=region,
            table_name=table_name
        )
        session.add(cluster)
        session.flush()  # Get the ID

    # Update cluster registry with latest data
    cluster.arn = cluster_data.get('arn')
    cluster.version = cluster_data.get('version')
    cluster.platform_version = cluster_data.get('platformVersion')
    cluster.endpoint = cluster_data.get('endpoint')
    cluster.status = cluster_data.get('status')

    created_at = cluster_data.get('createdAt')
    if isinstance(created_at, str):
        cluster.created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
    elif isinstance(created_at, datetime):
        cluster.created_at = created_at

    role_arn = cluster_data.get('roleArn')
    if isinstance(role_arn, str):
        cluster.role_arn = role_arn.replace('\n', ' ').strip()
    else:
        cluster.role_arn = None
    if cluster.role_arn and isinstance(cluster.role_arn, str):
        cluster.role_arn = cluster.role_arn.strip()

    cluster.tags = cluster_data.get('tags')
    cluster.health_issues = cluster_data.get('health_issues')
    cluster.eks_auto_mode = cluster_data.get('eks_auto_mode')
    cluster.networking = cluster_data.get('networking')
    cluster.oidc_provider_url = cluster_data.get('oidc_provider_url')
    cluster.security_insights = cluster_data.get('security_insights')
    cluster.workloads = cluster_data.get('workloads')

    if cluster_data.get('certificateAuthority'):
        cluster.certificate_authority_data = cluster_data['certificateAuthority'].get('data')

    cluster.last_updated = datetime.utcnow()

    # Update subresources in dedicated tables
    _update_cluster_subresources(session, cluster.table_name, cluster_data)

    # Debug print for verification
    print("[DEBUG] Final Cluster object before commit:")
    # print("  role_arn:", cluster.role_arn)
    # print("  nodegroups:", json.dumps(cluster_data.get('nodegroups_data', []), indent=2, default=str))
    # print("  addons:", json.dumps(cluster_data.get('addons', []), indent=2, default=str))

    session.commit()
    session.refresh(cluster)
    return cluster

def _update_cluster_subresources(session: Session, table_name: str, cluster_data: dict):
    """Update subresources in dedicated cluster tables"""
    
    # Update nodegroups
    _update_table_data(session, f"{table_name}_nodegroups", 
                      cluster_data.get('nodegroups_data', []),
                      lambda ng: {
                          'name': ng.get('name'),
                          'status': ng.get('status'),
                          'ami_type': ng.get('amiType'),
                          'instance_types': ng.get('instanceTypes'),
                          'release_version': ng.get('releaseVersion'),
                          'version': ng.get('version'),
                          'created_at': ng.get('createdAt'),
                          'desired_size': ng.get('desiredSize'),
                          'is_karpenter_node': ng.get('is_karpenter_node', False)
                      }, 'name')

    # Update addons
    _update_table_data(session, f"{table_name}_addons",
                      cluster_data.get('addons', []),
                      lambda a: {
                          'name': a.get('addonName'),
                          'version': a.get('addonVersion'),
                          'status': a.get('status'),
                          'pod_identity_display': a.get('pod_identity_display'),
                          'irsa_role_arn': a.get('irsa_role_arn')
                      }, 'name')

    # Update fargate profiles
    _update_table_data(session, f"{table_name}_fargate_profiles",
                      cluster_data.get('fargate_profiles', []),
                      lambda f: {'name': f.get('name'), 'status': f.get('status')},
                      'name')

    # Update access entries
    _update_table_data(session, f"{table_name}_access_entries",
                      cluster_data.get('access_entries', []),
                      lambda e: {
                          'principal_arn': e.get('principalArn'),
                          'type': e.get('type'),
                          'username': e.get('username'),
                          'groups': e.get('groups') or [],
                          'access_policies': e.get('access_policies') or []
                      }, 'principal_arn')

def _update_table_data(session: Session, table_name: str, incoming_data: list, data_transformer, key_field: str):
    """Generic function to update data in a cluster's dedicated table"""
    if not incoming_data:
        # Clear the table if no data
        session.execute(text(f"DELETE FROM {table_name}"))
        return

    # Get existing data - first get column names
    columns_result = session.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
    column_names = [col[1] for col in columns_result]  # col[1] is the column name
    
    existing_data = session.execute(text(f"SELECT * FROM {table_name}")).fetchall()
    existing_map = {}
    for row in existing_data:
        row_dict = dict(zip(column_names, row))
        key_value = row_dict.get(key_field)
        if key_value:
            existing_map[key_value] = row_dict

    # Transform incoming data
    incoming_map = {}
    for item in incoming_data:
        if isinstance(item, dict):
            transformed = data_transformer(item)
            key = transformed.get(key_field)
            if key:
                # Serialize JSON fields to strings for SQLite
                for k, v in transformed.items():
                    if isinstance(v, (list, dict)):
                        transformed[k] = json.dumps(v)
                incoming_map[key] = transformed

    # Delete items not in incoming data
    for key in existing_map:
        if key not in incoming_map:
            session.execute(text(f"DELETE FROM {table_name} WHERE {key_field} = :key"), {"key": key})

    # Insert or update items
    for key, data in incoming_map.items():
        if key in existing_map:
            # Update existing
            set_clause = ", ".join([f"{k} = :{k}" for k in data.keys()])
            params = data.copy()
            params['key'] = key
            session.execute(text(f"UPDATE {table_name} SET {set_clause} WHERE {key_field} = :key"), params)
        else:
            # Insert new
            columns = ", ".join(data.keys())
            placeholders = ", ".join([f":{k}" for k in data.keys()])
            session.execute(text(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"), data)

def get_all_clusters_summary(session: Session):
    clusters = session.query(ClusterRegistry).all()
    return [{
        "name": c.name, "account_id": c.account_id, "region": c.region, "version": c.version, "status": c.status,
        "createdAt": c.created_at.isoformat() if c.created_at else None,
        "health_status_summary": "HEALTHY" if not c.health_issues else "HAS_ISSUES",
        "upgrade_insight_status": "PASSING" if c.version and c.version >= "1.29" else "NEEDS_ATTENTION",
    } for c in clusters]

def get_cluster_details(session: Session, account_id: str, region: str, cluster_name: str):
    cluster = session.query(ClusterRegistry).filter(
        ClusterRegistry.account_id == account_id,
        ClusterRegistry.region == region,
        ClusterRegistry.name == cluster_name
    ).first()

    if not cluster:
        return None

    # Get cluster data from registry
    cluster_dict = {
        key: getattr(cluster, key)
        for key in cluster.__table__.columns.keys()
        if key != 'workloads' and key != 'table_name'
    }

    cluster_dict['createdAt'] = cluster.created_at
    cluster_dict['certificateAuthority'] = {'data': cluster.certificate_authority_data}

    # Get subresources from dedicated tables
    cluster_dict['nodegroups_data'] = _get_table_data(session, f"{cluster.table_name}_nodegroups")
    cluster_dict['addons'] = [
        {
            'addonName': a['name'],
            'addonVersion': a['version'],
            'status': a['status'],
            'pod_identity_display': a['pod_identity_display'],
            'irsa_role_arn': a['irsa_role_arn']
        } for a in _get_table_data(session, f"{cluster.table_name}_addons")
    ]
    cluster_dict['fargate_profiles'] = [
        {'name': f['name'], 'status': f['status']}
        for f in _get_table_data(session, f"{cluster.table_name}_fargate_profiles")
    ]

    # Access entries
    cluster_dict['access_entries'] = [
        {
            'principalArn': ae['principal_arn'],
            'type': ae['type'],
            'username': ae['username'],
            'groups': ae['groups'] or [],
            'access_policies': ae['access_policies'] or []
        } for ae in _get_table_data(session, f"{cluster.table_name}_access_entries")
    ]

    # Workloads
    cluster_dict['workloads'] = cluster.workloads

    # Error handling for UI
    if cluster.workloads and cluster.workloads.get('error'):
        cluster_dict['workloads_error'] = cluster.workloads.get('error')
    else:
        cluster_dict['workloads_error'] = None

    return cluster_dict

def _get_table_data(session: Session, table_name: str):
    """Get all data from a cluster's dedicated table"""
    try:
        # Get column names first
        columns_result = session.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
        column_names = [col[1] for col in columns_result]  # col[1] is the column name
        
        result = session.execute(text(f"SELECT * FROM {table_name}")).fetchall()
        rows = []
        for row in result:
            row_dict = dict(zip(column_names, row))
            # Deserialize JSON fields from strings
            for k, v in row_dict.items():
                if isinstance(v, str) and v.startswith(('{', '[')):
                    try:
                        row_dict[k] = json.loads(v)
                    except (json.JSONDecodeError, ValueError):
                        # Keep as string if not valid JSON
                        pass
            rows.append(row_dict)
        return rows
    except Exception as e:
        print(f"Warning: Could not read from table {table_name}: {e}")
        return []

def delete_cluster(session: Session, account_id: str, region: str, cluster_name: str):
    """Delete a cluster and its dedicated tables"""
    cluster = session.query(ClusterRegistry).filter(
        ClusterRegistry.account_id == account_id,
        ClusterRegistry.region == region,
        ClusterRegistry.name == cluster_name
    ).first()

    if cluster:
        # Drop dedicated tables
        drop_cluster_tables(account_id, region, cluster_name)
        
        # Remove from registry
        session.delete(cluster)
        session.commit()
        return True
    return False
