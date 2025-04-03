import unittest
import sys
import logging
import os
import subprocess
import importlib.util
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_and_install_dependencies():
    """Check if required dependencies are installed and install them if not."""
    logger.info("Checking for required dependencies...")
    
    base_dir = Path(__file__).parent.parent
    requirements_file = base_dir / "requirements.txt"
    
    if not requirements_file.exists():
        logger.warning("requirements.txt not found. Using default dependencies from setup.py.")
        dependencies = ['typing-extensions', 'duckdb']
    else:
        with open(requirements_file, 'r') as f:
            dependencies = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    
    missing_deps = []
    for dep in dependencies:
        package_name = dep.split('==')[0].split('>=')[0].split('<=')[0].strip()
        if importlib.util.find_spec(package_name) is None:
            missing_deps.append(dep)
    
    if missing_deps:
        logger.info(f"Installing missing dependencies: {', '.join(missing_deps)}")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing_deps)
            logger.info("Dependencies installed successfully.")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to install dependencies: {e}")
            return False
    else:
        logger.info("All required dependencies are already installed.")
    
    return True

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
    if check_and_install_dependencies():
        success = discover_and_run_tests()
        sys.exit(0 if success else 1)
    else:
        logger.error("Failed to install required dependencies. Exiting.")
        sys.exit(1)
