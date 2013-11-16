/*
 * cleanup.c - Enforces metric/host delete time. Helps keep
 *    memory usage trim and fit by deleting expired metrics from hash.
 *
 * ChangeLog:
 *
 * 25sept2002 - Federico Sacerdoti <fds@sdsc.edu>
 *    Original version.
 *
 * 16feb2003 - Federico Sacerdoti <fds@sdsc.edu>
 *    Made more efficient: O(n) from O(n^2) in
 *    worst case. Fixed bug in hash_foreach() that caused
 *    immortal metrics.
 * 
 * Sept2004 - Federico D. Sacerdoti <fds@sdsc.edu>
 *    Adapted for use in Gmetad.
 *
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <string.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>

#include <apr_time.h>

#include "ganglia.h"
#include "gmetad.h"

#include "conf.h"
#include "cmdline.h"

/* Interval (seconds) between runs */
#define RIEMANN_CB_INTERVAL 60

void *
circuit_breaker_thread(void *arg)
{
   for (;;) {

      apr_sleep(apr_time_from_sec(RIEMANN_CB_INTERVAL));

      debug_msg("Circuit breaker thread running...");

      if (riemann_circuit_breaker == RIEMANN_CB_OPEN && riemann_reset_timeout < apr_time_now ()) {

         printf ("Reset period expired, retry connection...\n");
         riemann_circuit_breaker = RIEMANN_CB_HALF_OPEN;

         riemann_tcp_socket = riemann_connect (riemann_server, riemann_port);

         if (riemann_tcp_socket < 0) {
            riemann_circuit_breaker = RIEMANN_CB_OPEN;
            riemann_reset_timeout = apr_time_now () + RIEMANN_TIMEOUT;      /* 60 seconds */
         } else {
           riemann_failures = 0;
           riemann_circuit_breaker = RIEMANN_CB_CLOSED;
         }
      }

    debug_msg("[riemann] circuit breaker is %s\n",
            riemann_circuit_breaker == RIEMANN_CB_OPEN ? "OPEN" :
            riemann_circuit_breaker == RIEMANN_CB_HALF_OPEN ? "HALF_OPEN"
            /* RIEMANN_CB_CLOSED */ : "CLOSED");
   }
}

