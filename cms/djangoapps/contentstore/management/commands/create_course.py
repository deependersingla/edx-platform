"""
Django management command to create a course in a specific modulestore
"""
from django.core.management.base import BaseCommand, CommandError
from django.contrib.auth.models import User
from xmodule.modulestore import ModuleStoreEnum
from contentstore.views.course import create_new_course_guts


def user_from_str(identifier):
    """
    Return a user identified by the given string. The string could be an email
    address, or a stringified integer corresponding to the ID of the user in
    the database. If no user could be found, a User.DoesNotExist exception
    will be raised.
    """
    try:
        user_id = int(identifier)
    except ValueError:
        return User.objects.get(email=identifier)

    return User.objects.get(id=user_id)


class Command(BaseCommand):
    """
    Create a course in a specific modulestore.
    """

    # can this query modulestore for the list of write accessible stores or does that violate command pattern?
    help = "Create a course in one of {}".format([ModuleStoreEnum.Type.mongo, ModuleStoreEnum.Type.split])
    args = "modulestore email org course run"

    def parse_args(self, *args):
        """
        Return a tuple of passed in values for (modulestore, user, org, course, run).
        """
        if len(args) < 2:
            raise CommandError(
                "create_course requires 5 arguments: "
                "a modulestore, user, org, course, run. Modulestore is one of {}".format(
                    [ModuleStoreEnum.Type.mongo, ModuleStoreEnum.Type.split]
                )
            )

        if args[0] not in [ModuleStoreEnum.Type.mongo, ModuleStoreEnum.Type.split]:
            raise CommandError(
                "Modulestore (first arg) must be one of {}".format(
                    [ModuleStoreEnum.Type.mongo, ModuleStoreEnum.Type.split]
                )
            )
        storetype = args[0]

        try:
            user = user_from_str(args[1])
        except User.DoesNotExist:
            raise CommandError("No user found identified by {}".format(args[1]))

        org = args[2]
        course = args[3]
        run = args[4]

        return storetype, user, org, course, run

    def handle(self, *args, **options):
        storetype, user, org, course, run = self.parse_args(*args)
        new_course = create_new_course_guts(storetype, user, org, course, run, {})
        self.stdout.write(u"Created {}".format(unicode(new_course.id)))
