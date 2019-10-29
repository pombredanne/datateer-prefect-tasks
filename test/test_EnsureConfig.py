from datateer.tasks.EnsureConfig import EnsureConfig

def test_increment():
    # arrange
    task = EnsureConfig()

    # act
    task.run('hmn-pipeline', 'prod', 'tap-s3-csv-axon.config', './tmp')

    # assert
    
