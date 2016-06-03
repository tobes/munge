import os.path
import tempfile
import subprocess
from datetime import datetime

from munge import config


def get_files(directory, extension=None, full_path=True):
    files = []
    for file in os.listdir(directory):
        if extension and not file.endswith(extension):
            continue
        if full_path:
            files.append(os.path.join(directory, file))
        else:
            files.append(file)
    return files


def vao():
    upload_path = os.path.join(config.DATA_PATH, 'upload', 'voa')
    store_path = os.path.join(config.DATA_PATH, 'store', 'voa')
    try:
        os.makedirs(store_path)
    except OSError:
        pass
    z = get_files(upload_path, '.zip')
    p = get_files(upload_path, '.txt')
    if len(z) != 1 or len(p) != 1:
        print 'files not there'
        return
    z = z[0]
    p = p[0]
    print z, p
    with open(p, 'r') as f:
        password = f.readline()
    print password
    tmp_dir = tempfile.mkdtemp()
    # unzip
    cmd = '7z e -p{} -o{} {}'.format(password, tmp_dir, z)
    print cmd
    subprocess.call(cmd.split(' '))
    vao_path = os.path.join(config.DATA_PATH, 'vao')
    # unzip data files
    for f in get_files(tmp_dir, '.Z'):
        cmd = '7z e -o{} {}'.format(vao_path, f)
        print cmd
        subprocess.call(cmd.split(' '))

        cmd = 'rm {}'.format(f)
        print cmd
        subprocess.call(cmd.split(' '))
    # remove tempdir
    cmd = 'rm -r {}'.format(tmp_dir)
    print cmd
    subprocess.call(cmd.split(' '))

    # remove tempdir
    ts = datetime.today().strftime('%Y-%m-%d')
    cmd = 'mv {} {}'.format(z, os.path.join(store_path, 'DATA_%s.zip' % ts))
    print cmd
    subprocess.call(cmd.split(' '))
    cmd = 'mv {} {}'.format(p, os.path.join(store_path, 'PW_%s.txt' % ts))
    print cmd
    subprocess.call(cmd.split(' '))






if __name__ == '__main__':
    vao()
