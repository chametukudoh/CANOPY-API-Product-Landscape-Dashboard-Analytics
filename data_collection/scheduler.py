"""Scheduling logic for automated data collection"""
import schedule
import time
import logging
from typing import Callable

logger = logging.getLogger(__name__)

class DataScheduler:
    """Manages scheduled data collection tasks"""
    
    def __init__(self):
        self.jobs = []
    
    def schedule_daily_collections(self, collect_func: Callable, times: list):
        """
        Schedule data collection at specific times daily
        
        Args:
            collect_func: Function to call for collection
            times: List of times in "HH:MM" format (e.g., ["06:00", "12:00", "18:00"])
        """
        for time_str in times:
            job = schedule.every().day.at(time_str).do(collect_func)
            self.jobs.append(job)
            logger.info(f"Scheduled collection at {time_str}")
    
    def schedule_enrichment(self, enrich_func: Callable, time: str = "02:00"):
        """Schedule product enrichment task"""
        job = schedule.every().day.at(time).do(enrich_func)
        self.jobs.append(job)
        logger.info(f"Scheduled enrichment at {time}")
    
    def schedule_metrics(self, metrics_func: Callable, time: str = "19:00"):
        """Schedule daily metrics computation"""
        job = schedule.every().day.at(time).do(metrics_func)
        self.jobs.append(job)
        logger.info(f"Scheduled metrics computation at {time}")
    
    def run(self):
        """Run the scheduler loop"""
        logger.info("Scheduler started. Press Ctrl+C to stop.")
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")