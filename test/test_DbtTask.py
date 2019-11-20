from datateer.tasks.DbtTask import generate_command_list

def test_run_command():
    commands = generate_command_list(['run'])
    assert 1 == len(commands)
    assert 'run' == commands[0][1]

def test_multiple_commands():
    commands = generate_command_list(['debug', 'deps', 'run'])
    assert 3 == len(commands)
    assert 'debug' == commands[0][1]
    assert 'deps' == commands[1][1]
    assert 'run' == commands[2][1]

def test_test_command():
    commands = generate_command_list(['test'])
    assert 1 == len(commands)
    assert 'test' == commands[0][1]

def test_models():
    commands = generate_command_list(['run'], models=['some_model+'])
    assert 1 == len(commands)
    assert 'run' == commands[0][1]
    assert '--models' == commands[0][2]
    assert 'some_model+' == commands[0][3]

    commands = generate_command_list(['test'], models=['+some_model'])
    assert 1 == len(commands)
    assert 'test' == commands[0][1]
    assert '--models' == commands[0][2]
    assert '+some_model' == commands[0][3]

def test_multiple_models():
    commands = generate_command_list(['run'], models=['some_model', '+other_model+'])
    assert 1 == len(commands)
    assert 'run' == commands[0][1]
    assert '--models' == commands[0][2]
    assert 'some_model' == commands[0][3]
    assert '+other_model+' == commands[0][4]