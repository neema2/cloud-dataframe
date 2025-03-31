import logging
import sys
import unittest

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

from cloud_dataframe.tests.integration import test_scalar_functions

if __name__ == "__main__":
    print("Running scalar function tests with debug logging enabled...")
    try:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_scalar_functions.TestScalarFunctionsIntegration)
        
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        
        if result.wasSuccessful():
            print("Tests completed successfully")
        else:
            print(f"Tests failed: {result.failures} failures, {result.errors} errors")
            sys.exit(1)
    except Exception as e:
        print(f"Error running tests: {e}")
        sys.exit(1)
