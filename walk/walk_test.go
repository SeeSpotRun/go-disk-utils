//heavily plaguerised from https://golang.org/src/path/filepath/path_test.go

package walk_test

import (
        "testing"
        "path/filepath"
        "os"
        "walk"
        "strings"
        "sync"
)

type Node struct {
	name    string
	entries []*Node // nil if the entry is a file
	mark    int
}

var tree = &Node{
	"testdata",
	[]*Node{
		{"a", nil, 0},
		{".b", []*Node {
                        {"w", nil, 0},  // file in hidden dir
                        }, 0},
		{"c", nil, 0},
		{
			"d",
			[]*Node{
				{".x", nil, 0}, // hidden file
				{"y", []*Node{}, 0},
				{
					strings.Repeat("z", 150),  // makes total path > 260 char windows limit
					[]*Node{
						{strings.Repeat("u", 150), nil, 0},
						{"v", nil, 0},
					},
					0,
				},
			},
			0,
		},
	},
	0,
}

func walkTree(n *Node, path string, f func(path string, n *Node)) {
	f(path, n)
	for _, e := range n.entries {
		walkTree(e, filepath.Join(path, e.name), f)
	}
}

func makeTree(t *testing.T) {
        root, err := walk.FixPath(tree.name)
        if err != nil {
                t.Fatalf("Error in FixPath: %s", err)
        }
	walkTree(tree, root, func(path string, n *Node) {
		if n.entries == nil {
			fd, err := os.Create(path)
			if err != nil {
				t.Errorf("makeTree: %v", err)
				return
			}
			fd.Close()
		} else {
			os.Mkdir(path, 0770)
		}
	})
}


func TestFileCh(t *testing.T) {
	makeTree(t)
        
        donec := make(chan struct{})
        
        root, err := walk.FixPath(tree.name)
        if err != nil {
                t.Fatalf("Error in FixPath: %s", err)
        }
        
        
        type testCase struct {
                options uint
                desc string
                roots []string
                expectfiles int
                expecterrors int
        }

        var tests = []testCase{
                testCase{walk.Defaults,
                        "defaults",
                        []string{tree.name},
                        4, 0},
                testCase{walk.HiddenFiles,
                        "hidden files",
                        []string{tree.name},
                        5, 0},
                testCase{walk.HiddenDirs, 
                        "hidden dirs",
                        []string{tree.name},
                        5, 0},
                testCase{walk.HiddenDirs + walk.HiddenFiles,
                        "hidden dirs and files",
                        []string{tree.name},
                        6, 0},
                testCase{walk.Defaults,
                        "repeated path path",
                        []string{tree.name, tree.name },
                        4, 0},
                testCase{walk.Defaults,
                        "overlapping path",
                        []string{tree.name, tree.name + "/d" },
                        4, 1},
                testCase{walk.Defaults,
                        "nonexistent path",
                        []string{tree.name, tree.name + "/m" },
                        4, 1},
                testCase{walk.Defaults,
                        "repeated path with trailing '/'",
                        []string{tree.name, tree.name + "/" },
                        4, 0},
                testCase{walk.Defaults,
                        "trailing '/'",
                        []string{tree.name + "/" },
                        4, 0},
                testCase{walk.Defaults,
                        "hidden root",
                        []string{tree.name + "/.b", tree.name},
                        5, 0},
                }

        
        for _, tc := range(tests) {
                files := []*walk.File{}
                errors := []error{}
                errc := make(chan error)
                var wg sync.WaitGroup
                wg.Add(1)
                go func() {
                        for e := range(errc) {
                                errors = append(errors, e)
                        }
                        wg.Done()
                }()
                for f := range(walk.FileCh(donec, errc, tc.roots, tc.options)) {
                        files = append(files, f)
                }
                close(errc)
                
                if len(files) != tc.expectfiles {
                        t.Errorf("Option %s: expected %d files, got %d", tc.desc, tc.expectfiles, len(files))
                }
                
                wg.Wait()
                if len(errors) != tc.expecterrors{
                        t.Errorf("Option %s: expected %d errors, got %d", tc.desc, tc.expecterrors, len(errors))
                }
        }
        
        
        // cleanup
	if err := os.RemoveAll(root); err != nil {
		t.Errorf("removeTree: %v", err)
	}
}
