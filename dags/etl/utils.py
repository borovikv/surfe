import os.path


def abs_path(path):
    base_folder = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    return os.path.join(base_folder, path.strip('/'))
