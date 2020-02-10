# How to add tests
    - In order to add tests open the test_extra.py file
    - Import the pytest library
    - When starting a test you have to add this decorator @pytest.mark.parametrize
    - After the decorator you have to add the parameter names names you want to test  
    - After the parameter names you can add as many arrays as you want with different
      variables you want to test
    - If you want to skip a test add this decorator @pytest.mark.skip(reason="skipping this")
    - Check the test_extra.py for more info


# How to run a test
    - Open a terminal and navigate to Pyophidia/Pyophidia directory
    - Type python -m pytest tests