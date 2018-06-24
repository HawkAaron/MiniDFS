# 1. insert dir
# 2. insert file id
# 3. get dir
# 4. tree view print
import os

class FileTree:
    """ File tree"""

    def __init__(self):
        self.tree = {'.': []}

    def _insert(self, d, dir, id=-1):
        """ insert dir into d, if id > 0, insert file into dir"""

        if dir not in list(d.keys()):
            d[dir] = {'.': []} # create dir
        if id > 0:
            d[dir]['.'].append(id) # add file id 

        return d[dir] # return dir

    def insert(self, path, id=-1):
        """ insert path into tree, if id > 0, add file into path"""

        path = path.split('/')

        # insert dir
        d = self.tree
        for p in path:
            d = self._insert(d, p)
        
        # add file id
        if id > 0: d['.'].append(id)
        # print(tree)

    def add(self, id):
        """ add file into current dir"""
        self.tree['.'].append(id)

    def get(self, dirs):
        """ get directory entry from tree"""
        tree = self.tree
        dirs = dirs.split('/')
        for dir in dirs:
            tree = tree.get(dir)
            if tree is None:
                break
        return tree

    def get_id_by_path(self, path, id_file_map):
        dir, name = os.path.split(path)
        dir = self.tree if dir == '' else self.get(dir)
        if dir is not None: 
            dir = dir['.']
            for file in dir:
                if name == id_file_map[file][0]:
                    return file
        return -1

    def view(self, id_file_map, pad=' ', root=''):
        def _view(tree, pad, root):
            print(pad[:-1] + '+-' + root + '/')
            pad += ' '
            dirs = list(tree.keys())
            cnt = 0

            files = tree['.']
            for file in files:
                print(pad + '|')
                name, length = id_file_map[file]
                print(pad + '+-' + '{} {} {}'.format(file, name, length))
            for dir in dirs:
                cnt += 1
                print(pad + '|')
                if dir == '.': continue
                if cnt == len(dirs):
                    _view(tree[dir], pad + ' ', dir)
                else:
                    _view(tree[dir], pad + '|', dir)
        _view(self.tree, pad, root)

def main():
    id_file_map = {0: ('file1.txt', 1), 
                1: ('file2.txt', 100),
                2: ('file3.txt', 200),
                3: ('readme.txt', 600),
                4: ('file7.txt', 700),
                5: ('file1.txt', 600),
                6: ('file.txt', 10000),
                7: ('file.txt', 100)}
    tree = FileTree()
    tree.add(0)
    tree.add(1)

    tree.insert('dir1', 2)
    tree.insert('dir1', 3)
    tree.insert('dir2', 4)
    tree.insert('dir2', 5)
    tree.insert('dir3/dir3')
    tree.insert('dir3/dir1/dir2', 6)
    tree.insert('dir3/dir2/dir2')
    tree.insert('dir3/dir2/dir2', 7)
    
    tree.view(id_file_map)

    print(tree.get('dir3/dir2'))

if __name__ == '__main__':
    main()
