/*
 * circuit_breaker.c
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

extern gmetad_config_t gmetad_config;

extern g_tcp_socket *riemann_tcp_socket;

/* Interval (seconds) between runs */
#define RIEMANN_CB_INTERVAL 1

void *
circuit_breaker_thread(void *arg)
{
   for (;;) {

      debug_msg("Circuit breaker thread running...");

      if (riemann_circuit_breaker == RIEMANN_CB_OPEN && riemann_reset_timeout < apr_time_now ()) {

         printf ("Reset period expired, retry connection...\n");
         riemann_circuit_breaker = RIEMANN_CB_HALF_OPEN;

         riemann_tcp_socket = init_riemann_tcp_socket (gmetad_config.riemann_server, gmetad_config.riemann_port);

         if (riemann_tcp_socket == NULL) {
            riemann_circuit_breaker = RIEMANN_CB_OPEN;
            riemann_reset_timeout = apr_time_now () + RIEMANN_TIMEOUT;
         } else {
           riemann_failures = 0;
           riemann_circuit_breaker = RIEMANN_CB_CLOSED;
         }
      }

      debug_msg("[riemann] circuit breaker is %s\n",
            riemann_circuit_breaker == RIEMANN_CB_OPEN ? "OPEN" :
            riemann_circuit_breaker == RIEMANN_CB_HALF_OPEN ? "HALF_OPEN"
            /* RIEMANN_CB_CLOSED */ : "CLOSED");

      apr_sleep(apr_time_from_sec(RIEMANN_CB_INTERVAL));
   }
}

