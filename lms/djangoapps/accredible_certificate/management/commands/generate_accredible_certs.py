"""
from management command to find all students that need certificates for
courses that have finished, and put their cert requests on the queue.
"""
from django.core.management.base import BaseCommand, CommandError
from certificates.models import certificate_status_for_student
from accredible_certificate.queue import CertificateGeneration
from django.contrib.auth.models import User
from optparse import make_option
from django.conf import settings
from opaque_keys import InvalidKeyError
from opaque_keys.edx.keys import CourseKey
from opaque_keys.edx.locations import SlashSeparatedCourseKey
from xmodule.course_module import CourseDescriptor
from xmodule.modulestore.django import modulestore
from certificates.models import CertificateStatuses
import datetime
from pytz import UTC



class Command(BaseCommand):

    help = """
    Find all students that need certificates for courses that have finished and
    put their cert requests on the queue.

    If --user is given, only grade and certify the requested username.

    Use the --noop option to test without actually putting certificates on the
    queue to be generated.
    """

    option_list = BaseCommand.option_list + (
        make_option('-c', '--course',
                    metavar='COURSE_ID',
                    dest='course',
                    default=False,
                    help='Grade and generate certificates '
                    'for a specific course'),
        make_option('-f', '--force-gen',
                    metavar='STATUS',
                    dest='force',
                    default=False,
                    help='Will generate new certificates for only those users '
                    'whose entry in the certificate table matches STATUS. '
                    'STATUS can be generating, unavailable, deleted, error '
                    'or notpassing.'),
        make_option('-a', '--api_key',
                    metavar='API_KEY',
                    dest='api_key',
                    default=None,
                    help='API key for accredible Certificate, if don\'t have one'
                    'Visit https://accredible.com/issuer/sign_up and get one'),    
    )

    def handle(self, *args, **options):

        # Will only generate a certificate if the current
        # status is in the unavailable state, can be set
        # to something else with the force flag

        if options['force']:
            valid_statuses = getattr(CertificateStatuses, options['force'])
        else:
            valid_statuses = [CertificateStatuses.unavailable]

        # Print update after this many students

        STATUS_INTERVAL = 500

        if options['course']:
            # try to parse out the course from the serialized form
            try:
                course = CourseKey.from_string(options['course'])
            except InvalidKeyError:
                print("Course id {} could not be parsed as a CourseKey; falling back to SSCK.from_dep_str".format(options['course']))
                course = SlashSeparatedCourseKey.from_deprecated_string(options['course'])
            ended_courses = [course]
        else:
            raise CommandError("You must specify a course")
        
        if options['api_key']:
            api_key = options['api_key']
        else:
            raise CommandError("You must give a api_key, if don't have one visit: https://accredible.com/issuer/sign_up")

        for course_key in ended_courses:
            # prefetch all chapters/sequentials by saying depth=2
            course = modulestore().get_course(course_key, depth=2)

            print "Fetching enrolled students for {0}".format(course_key.to_deprecated_string())
            enrolled_students = User.objects.filter(
                courseenrollment__course_id=course_key)

            xq = CertificateGeneration(api_key=api_key)
            total = enrolled_students.count()
            print "Total number of students: " + str(total) 
            for student in enrolled_students:
                if certificate_status_for_student(
                    student, course_key)['status'] in valid_statuses:
                      ret = xq.add_cert(student, course_key, course=course)
                      print ret

         


