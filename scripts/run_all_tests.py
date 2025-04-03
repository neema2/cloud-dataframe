import unittest
import sys
import logging
import os
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def discover_and_run_tests():
    """Discover and run all tests in the tests directory."""
    logger.info("Discovering all tests in cloud_dataframe/tests directory...")
    
    base_dir = Path(__file__).parent.parent
    test_dir = base_dir / "cloud_dataframe" / "tests"
    
    loader = unittest.TestLoader()
    suite = loader.discover(str(test_dir), pattern="test_*.py")
    
    logger.info(f"Running all discovered tests...")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    
    logger.info("=" * 80)
    logger.info("TEST SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total tests run: {result.testsRun}")
    logger.info(f"Tests passed: {result.testsRun - len(result.failures) - len(result.errors)}")
    logger.info(f"Tests failed: {len(result.failures)}")
    logger.info(f"Tests with errors: {len(result.errors)}")
    
    if result.failures:
        logger.info("\nFAILURES:")
        for i, (test, traceback) in enumerate(result.failures, 1):
            logger.info(f"{i}. {test}")
            logger.info(f"   Error: {traceback.split('AssertionError:')[-1].strip()[:200]}...")
    
    if result.errors:
        logger.info("\nERRORS:")
        for i, (test, traceback) in enumerate(result.errors, 1):
            logger.info(f"{i}. {test}")
            logger.info(f"   Error: {traceback.split('Error:')[-1].strip()[:200]}...")
    
    return result.wasSuccessful()

if __name__ == "__main__":
    success = discover_and_run_tests()
    sys.exit(0 if success else 1)
