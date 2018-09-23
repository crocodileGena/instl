#!/usr/bin/env python3.6


import sys
import os
from pathlib import Path
import unittest
import shutil
import stat
import ctypes
import io
import contextlib
import filecmp
import random
import string
from collections import namedtuple

import utils
from pybatch import *
from pybatch import PythonBatchCommandAccum
from pybatch.copyBatchCommands import RsyncClone
from configVar import config_vars

current_os_names = utils.get_current_os_names()
os_family_name = current_os_names[0]
os_second_name = current_os_names[0]
if len(current_os_names) > 1:
    os_second_name = current_os_names[1]

config_vars["__CURRENT_OS_NAMES__"] = current_os_names


from testPythonBatch import *


class TestPythonBatchRemove(unittest.TestCase):
    def __init__(self, which_test="apple"):
        super().__init__(which_test)
        self.pbt = TestPythonBatch(self, which_test)

    def setUp(self):
        self.pbt.setUp()

    def tearDown(self):
        self.pbt.tearDown()

    def test_RmFile_repr(self):
        obj = RmFile(r"\just\remove\me\already")
        obj_recreated = eval(repr(obj))
        diff_explanation = obj.explain_diff(obj_recreated)
        self.assertEqual(obj, obj_recreated, f"RmFile.repr did not recreate RmFile object correctly: {diff_explanation}")

    def test_RmDir_repr(self):
        obj = RmDir(r"\just\remove\me\already")
        obj_recreated = eval(repr(obj))
        diff_explanation = obj.explain_diff(obj_recreated)
        self.assertEqual(obj, obj_recreated, f"RmDir.repr did not recreate RmDir object correctly: {diff_explanation}")

    def test_remove(self):
        """ Create a folder and fill it with random files.
            1st try to remove the folder with RmFile which should fail and raise exception
            2nd try to remove the folder with RmDir which should work
        """
        dir_to_remove = self.pbt.path_inside_test_folder("remove-me")
        self.assertFalse(dir_to_remove.exists())

        self.pbt.batch_accum.clear()
        self.pbt.batch_accum += MakeDirs(dir_to_remove)
        with self.pbt.batch_accum.sub_accum(Cd(dir_to_remove)) as sub_bc:
            sub_bc += MakeRandomDirs(num_levels=3, num_dirs_per_level=5, num_files_per_dir=7, file_size=41)
        self.pbt.batch_accum += RmFile(dir_to_remove)  # RmFile should not remove a folder
        self.pbt.exec_and_capture_output(expected_exception=PermissionError)
        self.assertTrue(dir_to_remove.exists())

        self.pbt.batch_accum.clear()
        self.pbt.batch_accum += RmDir(dir_to_remove)
        self.pbt.exec_and_capture_output()
        self.assertFalse(dir_to_remove.exists())

    def test_RemoveEmptyFolders_repr(self):
        with self.assertRaises(TypeError):
            obj = RemoveEmptyFolders()

        obj = RemoveEmptyFolders("/per/pen/di/cular")
        obj_recreated =eval(repr(obj))
        diff_explanation = obj.explain_diff(obj_recreated)
        self.assertEqual(obj, obj_recreated, f"RemoveEmptyFolders.repr did not recreate MacDoc object correctly: {diff_explanation}")

        obj = RemoveEmptyFolders("/per/pen/di/cular", [])
        obj_recreated =eval(repr(obj))
        diff_explanation = obj.explain_diff(obj_recreated)
        self.assertEqual(obj, obj_recreated, f"RemoveEmptyFolders.repr did not recreate MacDoc object correctly: {diff_explanation}")

        obj = RemoveEmptyFolders("/per/pen/di/cular", ['async', 'await'])
        obj_recreated =eval(repr(obj))
        diff_explanation = obj.explain_diff(obj_recreated)
        self.assertEqual(obj, obj_recreated, f"RemoveEmptyFolders.repr did not recreate MacDoc object correctly: {diff_explanation}")

    def test_RemoveEmptyFolders(self):
        folder_to_remove = self.pbt.path_inside_test_folder("folder-to-remove")
        file_to_stay = folder_to_remove.joinpath("paramedic")

        # create the folder, with sub folder and one known file
        self.pbt.batch_accum.clear()
        self.pbt.batch_accum += MakeDirs(folder_to_remove)
        with self.pbt.batch_accum.sub_accum(Cd(folder_to_remove)) as cd_accum:
            cd_accum += Touch(file_to_stay.name)
            cd_accum += MakeRandomDirs(num_levels=3, num_dirs_per_level=2, num_files_per_dir=0, file_size=41)
        self.pbt.exec_and_capture_output("create empty folders")
        self.assertTrue(os.path.isdir(folder_to_remove), f"{self.pbt.which_test} : folder to remove was not created {folder_to_remove}")
        self.assertTrue(os.path.isfile(file_to_stay), f"{self.pbt.which_test} : file_to_stay was not created {file_to_stay}")

        # remove empty folders, top folder and known file should remain
        self.pbt.batch_accum.clear()
        self.pbt.batch_accum += RemoveEmptyFolders(folder_to_remove, files_to_ignore=['.DS_Store'])
        # removing non existing folder should not be a problem
        self.pbt.batch_accum += RemoveEmptyFolders("kajagogo", files_to_ignore=['.DS_Store'])
        self.pbt.exec_and_capture_output("remove almost empty folders")
        self.assertTrue(os.path.isdir(folder_to_remove), f"{self.pbt.which_test} : folder was removed although it had a legit file {folder_to_remove}")
        self.assertTrue(os.path.isfile(file_to_stay), f"{self.pbt.which_test} : file_to_stay was removed {file_to_stay}")

        # remove empty folders, with known file ignored - so to folder should be removed
        self.pbt.batch_accum.clear()
        self.pbt.batch_accum += RemoveEmptyFolders(folder_to_remove, files_to_ignore=['.DS_Store', "paramedic"])
        self.pbt.exec_and_capture_output("remove empty folders")
        self.assertFalse(os.path.isdir(folder_to_remove), f"{self.pbt.which_test} : folder was not removed {folder_to_remove}")
        self.assertFalse(os.path.isfile(file_to_stay), f"{self.pbt.which_test} : file_to_stay was not removed {file_to_stay}")

    def test_RmGlob_repr(self):
        obj = RmGlob("/lo/lla/pa/loo/za", "*.pendicular")
        obj_recreated = eval(repr(obj))
        diff_explanation = obj.explain_diff(obj_recreated)
        self.assertEqual(obj, obj_recreated, f"RmGlob.repr did not recreate MacDoc object correctly: {diff_explanation}")

    def test_RmGlob(self):
        folder_to_glob = self.pbt.path_inside_test_folder("folder-to-glob")

        pattern = "?b*.k*"
        files_that_should_be_removed = ["abc.kif", "cba.kmf"]
        files_that_should_not_be_removed = ["acb.kof", "bac.kaf", "bca.kuf", "cab.kef"]

        self.pbt.batch_accum.clear()
        self.pbt.batch_accum += MakeDirs(folder_to_glob)
        with self.pbt.batch_accum.sub_accum(Cd(folder_to_glob)) as cd_accum:
            for f in files_that_should_be_removed + files_that_should_not_be_removed:
                cd_accum += Touch(f)

        self.pbt.batch_accum += RmGlob(os.fspath(folder_to_glob), pattern)
        self.pbt.exec_and_capture_output()

        for f in files_that_should_be_removed:
            fp = Path(folder_to_glob, f)
            self.assertFalse(fp.is_file(), f"{self.pbt.which_test} : file was not removed {fp}")

        for f in files_that_should_not_be_removed:
            fp = Path(folder_to_glob, f)
            self.assertTrue(fp.is_file(), f"{self.pbt.which_test} : file was removed {fp}")
