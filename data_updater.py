import asyncio
import logging
from sqlalchemy.orm import Session
from datetime import datetime

# FIX: Import the 'ClusterRegistry' model directly to use it for queries.
from database import SessionLocal, DataUpdateLog, update_cluster_data, get_all_clusters_summary, ClusterRegistry, delete_cluster
from aws_data_fetcher import get_live_eks_data, get_single_cluster_details, get_role_arn_for_account

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def update_all_data():
    """Fetches all cluster data from AWS and updates the database."""
    db_session: Session = SessionLocal()
    logging.info("Starting background data update...")
    try:
        raw_data = get_live_eks_data(user_groups=["admin"], group_map_str="")
        if raw_data.get("errors"):
            logging.error(f"Errors during cluster discovery: {raw_data['errors']}")

        clusters_from_aws = raw_data.get("clusters", [])
        logging.info(f"Discovered {len(clusters_from_aws)} clusters from AWS APIs.")

        clusters_in_db = { (c['account_id'], c['region'], c['name']) for c in get_all_clusters_summary(db_session) }
        clusters_from_aws_set = set()
        
        # Use a temporary session for the concurrent tasks. The main session will be used for logging and deletions.
        async def run_update_task(c_raw):
            session = SessionLocal()
            try:
                account_id = c_raw["arn"].split(':')[4]
                region = c_raw["region"]
                name = c_raw["name"]
                clusters_from_aws_set.add((account_id, region, name))
                await update_single_cluster_data(session, account_id, region, name)
            except (KeyError, IndexError) as e:
                logging.error(f"Could not parse essential info from raw cluster data: {c_raw}. Error: {e}")
                raise  # Re-raise to be caught by gather
            finally:
                session.close()

        tasks = [run_update_task(c_raw) for c_raw in clusters_from_aws]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        success_count = sum(1 for r in results if not isinstance(r, Exception))
        fail_count = len(results) - success_count
        
        # Handle deletions: clusters in DB but not in AWS anymore
        clusters_to_delete = clusters_in_db - clusters_from_aws_set
        if clusters_to_delete:
            logging.info(f"Found {len(clusters_to_delete)} clusters to delete.")
            for acc_id, reg, clus_name in clusters_to_delete:
                # Use the new delete_cluster function which handles table cleanup
                if delete_cluster(db_session, acc_id, reg, clus_name):
                    logging.info(f"Deleted cluster and its tables: {clus_name} in {reg}")
                else:
                    logging.warning(f"Could not delete cluster: {clus_name} in {reg}")

        details_message = f"Update complete. Success: {success_count}, Failed: {fail_count}, Deleted: {len(clusters_to_delete)}."
        logging.info(details_message)
        
        log_entry = DataUpdateLog(status='SUCCESS', details=details_message)
        db_session.add(log_entry)
        db_session.commit()

    except Exception as e:
        logging.error(f"Fatal error during background data update: {e}", exc_info=True)
        log_entry = DataUpdateLog(status='FAIL', details=str(e))
        db_session.add(log_entry)
        db_session.commit()
    finally:
        db_session.close()

async def update_single_cluster_data(db_session: Session, account_id: str, region: str, cluster_name: str):
    """Fetches and updates data for a single cluster. Manages its own session."""
    logging.info(f"Updating details for cluster: {cluster_name} in {region}")
    try:
        role_arn = get_role_arn_for_account(account_id)
        detailed_data = get_single_cluster_details(account_id, region, cluster_name, role_arn)
        
        if detailed_data and not detailed_data.get("errors"):
            update_cluster_data(db_session, detailed_data)
            logging.info(f"Successfully updated {cluster_name}.")
        elif detailed_data and detailed_data.get("errors"):
             raise Exception(f"Failed to get details for {cluster_name}: {detailed_data.get('errors')}")
        else:
             raise Exception(f"Received no data for {cluster_name}")
    except Exception as e:
        logging.error(f"Could not update cluster {cluster_name}: {e}", exc_info=False)
        # Re-raise the exception so asyncio.gather can capture it as a failure
        raise
