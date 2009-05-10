/* Some simple code just to check that installation is working */

#include <drmaa.h>

int main (int argc, char **argv) {
  char error[DRMAA_ERROR_STRING_BUFFER];
  int errnum = 0;
  
  errnum = drmaa_init (NULL, error, DRMAA_ERROR_STRING_BUFFER);
  
  if (errnum != DRMAA_ERRNO_SUCCESS) {
    fprintf (stderr, "Could not initialize the DRMAA library: %s\n", error);
    return 1;
  }
  printf ("DRMAA library was started successfully\n");
  
  errnum = drmaa_allocate_job_template (&jt, error, DRMAA_ERROR_STRING_BUFFER);
  
  if (errnum != DRMAA_ERRNO_SUCCESS) {
	  fprintf (stderr, "Could not create job template: %s\n", error);
  }
  else {
	  errnum = drmaa_set_attribute (jt, DRMAA_REMOTE_COMMAND, "/bin/sleep",
									error, DRMAA_ERROR_STRING_BUFFER);
	  
	  if (errnum != DRMAA_ERRNO_SUCCESS) {
          fprintf (stderr, "Could not set attribute \"%s\": %s\n",
                   DRMAA_REMOTE_COMMAND, error);
	  }
	  else {
          const char *args[2] = {"10", NULL};
          
          errnum = drmaa_set_vector_attribute (jt, DRMAA_V_ARGV, args, error,
                                               DRMAA_ERROR_STRING_BUFFER);
	  }
	  
	  if (errnum != DRMAA_ERRNO_SUCCESS) {
          fprintf (stderr, "Could not set attribute \"%s\": %s\n",
                   DRMAA_REMOTE_COMMAND, error);
	  }
	  else {
          char jobid[DRMAA_JOBNAME_BUFFER];
		  
          errnum = drmaa_run_job (jobid, DRMAA_JOBNAME_BUFFER, jt, error,
                                  DRMAA_ERROR_STRING_BUFFER);
		  
          if (errnum != DRMAA_ERRNO_SUCCESS) {
			  fprintf (stderr, "Could not submit job: %s\n", error);
          }
          else {
			  printf ("Your job has been submitted with id %s\n", jobid);
          }
	  } /* else */
	  
	  errnum = drmaa_delete_job_template (jt, error, DRMAA_ERROR_STRING_BUFFER);
	  
	  if (errnum != DRMAA_ERRNO_SUCCESS) {
          fprintf (stderr, "Could not delete job template: %s\n", error);
	  }
  } /* else */
  
  errnum = drmaa_exit (error, DRMAA_ERROR_STRING_BUFFER);
  
  if (errnum != DRMAA_ERRNO_SUCCESS) {
	  fprintf (stderr, "Could not shut down the DRMAA library: %s\n", error);
	  return 1;
  }
  printf ("DRMAA quit successfully\n");
  return 0;
}
