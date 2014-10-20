"""
Unittests for creating a course in an chosen modulestore
"""
import unittest
import ddt
from django.core.management import CommandError, call_command

from contentstore.management.commands.create_course import Command
from xmodule.modulestore import ModuleStoreEnum
from xmodule.modulestore.tests.django_utils import ModuleStoreTestCase
from xmodule.modulestore.django import modulestore


class TestArgParsing(unittest.TestCase):
    """
    Tests for parsing arguments for the `migrate_to_split` management command
    """
    def setUp(self):
        self.command = Command()

    def test_no_args(self):
        errstring = "create_course requires 5 arguments"
        with self.assertRaisesRegexp(CommandError, errstring):
            self.command.handle('create_course')

    def test_invalid_store(self):
        with self.assertRaises(CommandError):
            self.command.handle("foo", "user@foo.org", "org", "course", "run")

    def test_xml_store(self):
        with self.assertRaises(CommandError):
            self.command.handle(ModuleStoreEnum.Type.xml, "user@foo.org", "org", "course", "run")

    def test_nonexistent_user_id(self):
        errstring = "No user found identified by 99"
        with self.assertRaisesRegexp(CommandError, errstring):
            self.command.handle("split", "99", "org", "course", "run")

    def test_nonexistent_user_email(self):
        errstring = "No user found identified by fake@example.com"
        with self.assertRaisesRegexp(CommandError, errstring):
            self.command.handle("mongo", "fake@example.com", "org", "course", "run")


@ddt.ddt
class TestMigrateToSplit(ModuleStoreTestCase):
    """
    Unit tests for migrating a course from old mongo to split mongo
    """

    def setUp(self):
        super(TestMigrateToSplit, self).setUp(create_user=True)

    @ddt.data(ModuleStoreEnum.Type.mongo, ModuleStoreEnum.Type.split)
    def test_all_stores_user_email(self, store):
        call_command(
            "create_course",
            store,
            str(self.user.email),
            "org", "course", "run"
        )
        new_key = modulestore().make_course_key("org", "course", "run")
        self.assertTrue(
            modulestore().has_course(new_key),
            "Could not find course in {}".format(store)
        )
        self.assertEqual(store, modulestore()._get_modulestore_for_courseid(new_key).get_modulestore_type())
