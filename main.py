"""
Autonomous Algorithmic Trading System - Main Controller
Manages the complete lifecycle of trading algorithm generation, testing, and deployment.
"""

import logging
import sys
from datetime import datetime
from typing import Dict, List, Optional, Any
import firebase_admin
from firebase_admin import firestore, credentials
from concurrent.futures import ThreadPoolExecutor
import asyncio

# Local imports
from config.settings import FIREBASE_CREDENTIALS, LOG_LEVEL
from modules.algorithm_generator import AlgorithmGenerator
from modules.backtesting_engine import BacktestingEngine
from modules.deployment_manager import DeploymentManager
from modules.performance_monitor import PerformanceMonitor
from utils.data_fetcher import MarketDataFetcher

class TradingSystemMaster:
    """Main controller for the autonomous trading ecosystem"""
    
    def __init__(self):
        self._setup_logging()
        self._initialize_firebase()
        self._initialize_modules()
        self.is_running = False
        
    def _setup_logging(self) -> None:
        """Configure comprehensive logging system"""
        logging.basicConfig(
            level=getattr(logging, LOG_LEVEL),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(f'logs/trading_system_{datetime.now().strftime("%Y%m%d")}.log')
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("Logging system initialized")
        
    def _initialize_firebase(self) -> None:
        """Initialize Firebase for state management and real-time data"""
        try:
            if not firebase_admin._apps:
                cred = credentials.Certificate(FIREBASE_CREDENTIALS)
                firebase_admin.initialize_app(cred)
                self.logger.info("Firebase initialized successfully")
            self.db = firestore.client()
            
            # Initialize Firestore collections if they don't exist
            self._initialize_firestore_structure()
            
        except Exception as e:
            self.logger.error(f"Firebase initialization failed: {e}")
            raise
            
    def _initialize_firestore_structure(self) -> None:
        """Create required Firestore collections"""
        required_collections = [
            'trading_algorithms',
            'backtest_results',
            'live_deployments',
            'performance_metrics',
            'system_state'
        ]
        
        for collection in required_collections:
            # Create collection reference - Firestore creates implicitly on first write
            doc_ref = self.db.collection(collection).document('_metadata')
            if not doc_ref.get().exists:
                doc_ref.set({'created_at': datetime.now().isoformat()})
                self.logger.info(f"Initialized Firestore collection: {collection}")
                
    def _initialize_modules(self) -> None:
        """Initialize all system modules"""
        self.logger.info("Initializing system modules...")
        
        self.data_fetcher = MarketDataFetcher()
        self.algorithm_generator = AlgorithmGenerator(self.db)
        self.backtesting_engine = BacktestingEngine(self.db)
        self.deployment_manager = DeploymentManager(self.db)
        self.performance_monitor = PerformanceMonitor(self.db)
        
        self.logger.info("All modules initialized successfully")
        
    async def run(self) -> None:
        """Main execution loop"""
        self.is_running = True
        self.logger.info("Starting autonomous trading system...")
        
        try:
            # Run in parallel using ThreadPoolExecutor for CPU-bound tasks
            with ThreadPoolExecutor(max_workers=5) as executor:
                loop = asyncio.get_event_loop()
                
                tasks = [
                    loop.run_in_executor(executor, self._generation_cycle),
                    loop.run_in_executor(executor, self._evaluation_cycle),
                    loop.run_in_executor(executor, self._deployment_cycle),
                    loop.run_in_executor(executor, self._monitoring_cycle)
                ]
                
                await asyncio.gather(*tasks)
                
        except KeyboardInterrupt:
            self.logger.info("Shutdown signal received")
        except Exception as e:
            self.logger.error(f"System failure: {e}")
        finally:
            await self.shutdown()
            
    def _generation_cycle(self) -> None:
        """Generate new trading algorithms using RL"""
        while self.is_running:
            try:
                self.logger.info("Starting algorithm generation cycle")
                
                # Generate new algorithms based on market conditions
                algorithms = self.algorithm_generator.generate_algorithms()
                
                for algo in algorithms:
                    # Store in Firestore
                    algo_ref = self.db.collection('trading_algorithms').document()
                    algo_ref.set({
                        **algo,
                        'created_at': datetime.now().isoformat(),
                        'status': 'generated',
                        'version': 1
                    })
                    self.logger.info(f"Generated algorithm {algo_ref.id}")
                    
                # Wait before next generation cycle
                asyncio.sleep(300)  # 5 minutes
                
            except Exception as e