/*
 * Copyright 2012 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <Python.h>
#include <time.h>


#ifdef __MACH__  /* MacOS X doesn't have clock_gettime() */


#include <mach/mach.h>
#include <mach/mach_time.h>

static PyObject *monotonic(PyObject *self, PyObject *args)
{
  static double factor;
  uint64_t now;

  if (!PyArg_ParseTuple(args, ""))
    return NULL;

  if (!factor) {
    mach_timebase_info_data_t timebase;
    mach_timebase_info(&timebase);
    factor = 1.0 * timebase.numer / timebase.denom / 1e9;
  }

  now = mach_absolute_time();
  return Py_BuildValue("d", (double)(now * factor));
}


#else  /* !__MACH__, so try POSIX.1-2001 */


#ifdef CLOCK_MONOTONIC_RAW
# define WHICH_CLOCK  CLOCK_MONOTONIC_RAW
#else
# define WHICH_CLOCK  CLOCK_MONOTONIC
#endif

static PyObject *monotonic(PyObject *self, PyObject *args)
{
  struct timespec ts;

  if (!PyArg_ParseTuple(args, ""))
    return NULL;

#ifdef CLOCK_MONOTONIC_RAW
  /*
   * Even if defined, the running kernel might not support it, in which case
   * this will return nonzero.  Then we'll fall back to CLOCK_MONOTONIC.
   * CLOCK_MONOTONIC_RAW is slightly better, if supported, because it takes
   * no adjustments at all (not even speed adjustments or leap seconds).
   */
  if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts) < 0)
#endif
  if (clock_gettime(CLOCK_MONOTONIC, &ts) < 0)
    return PyErr_SetFromErrno(PyExc_OSError);

  return Py_BuildValue("d", (double)(ts.tv_sec * 1.0 + ts.tv_nsec / 1e9));
}


#endif  /* __MACH__ */


static PyMethodDef _monotime_methods[] = {
    { "monotonic", monotonic, METH_VARARGS,
        "Returns a strictly increasing number of seconds since\n"
        "an arbitrary start point." },
    { NULL, NULL, 0, NULL },  // sentinel
};


PyMODINIT_FUNC init_monotime(void)
{
    Py_InitModule("_monotime", _monotime_methods);
}
