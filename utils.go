package anchordb

import "os"

func syncDir(dirPath string) error {
    dir, err := os.Open(dirPath)
    if err != nil {
        return err
    }
    defer dir.Close()

    if err := dir.Sync(); err != nil {
        return err
    }

    return nil
}