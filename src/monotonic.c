#include <time.h>

#ifdef __MACH__  /* MacOS X doesn't have clock_gettime() */

#include <mach/mach.h>
#include <mach/mach_time.h>

double monotonic(void)
{
  static double factor;
  uint64_t now;

  if (!factor) {
    mach_timebase_info_data_t timebase;
    mach_timebase_info(&timebase);
    factor = 1.0 * timebase.numer / timebase.denom / 1e9;
  }

  now = mach_absolute_time();
  return (double)(now * factor);
}


#else  /* !__MACH__, so try POSIX.1-2001 */

#ifdef CLOCK_MONOTONIC_RAW
# define WHICH_CLOCK  CLOCK_MONOTONIC_RAW
#else
# define WHICH_CLOCK  CLOCK_MONOTONIC
#endif

double monotonic(void)
{
  struct timespec ts;

#ifdef CLOCK_MONOTONIC_RAW
  /*
   * Even if defined, the running kernel might not support it, in which case
   * this will return nonzero.  Then we'll fall back to CLOCK_MONOTONIC.
   * CLOCK_MONOTONIC_RAW is slightly better, if supported, because it takes
   * no adjustments at all (not even speed adjustments or leap seconds).
   */
  if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts) < 0)
#else
  if (clock_gettime(CLOCK_MONOTONIC, &ts) < 0)
#endif
    return -1.0;

  return (double)(ts.tv_sec * 1.0 + ts.tv_nsec / 1e9);
}


#endif  /* __MACH__ */

