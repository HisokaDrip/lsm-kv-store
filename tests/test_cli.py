from lsmkv.__main__ import main


def test_cli_roundtrip(tmp_path, capsys):
    d = str(tmp_path)
    assert main([d, "put", "user:1", "mazy"]) == 0
    assert main([d, "put", "user:2", "someone"]) == 0
    assert main([d, "del", "user:2"]) == 0
    capsys.readouterr()
    main([d, "scan"])
    assert capsys.readouterr().out == "user:1\tmazy\n"
    main([d, "get", "user:2"])
    assert "(not found)" in capsys.readouterr().out
