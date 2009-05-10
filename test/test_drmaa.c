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
  
  errnum = drmaa_exit (error, DRMAA_ERROR_STRING_BUFFER);
  
  if (errnum != DRMAA_ERRNO_SUCCESS) {
    fprintf (stderr, "Could not shut down the DRMAA library: %s\n", error);
    return 1;
  }
  printf ("DRMAA quit successfully\n");
  return 0;
}
