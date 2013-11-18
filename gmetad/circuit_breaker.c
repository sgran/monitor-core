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

extern int riemann_circuit_breaker;
extern int riemann_reset_timeout;
extern int riemann_failures;

extern g_tcp_socket* init_riemann_tcp_socket (const char *hostname, uint16_t port);

/* Interval (seconds) between runs */
#define RIEMANN_CB_INTERVAL 10

void *
circuit_breaker_thread(void *arg)
{
   for (;;) {

      if (riemann_circuit_breaker == RIEMANN_CB_OPEN && riemann_reset_timeout < apr_time_now ()) {

         debug_msg ("Reset period expired, retry connection...");
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

      debug_msg("[riemann] circuit breaker is %s",
            riemann_circuit_breaker == RIEMANN_CB_CLOSED ? "CLOSED" :
            riemann_circuit_breaker == RIEMANN_CB_OPEN ?   "OPEN"
                              /* RIEMANN_CB_HALF_OPEN */ : "HALF_OPEN");

      apr_sleep(apr_time_from_sec(RIEMANN_CB_INTERVAL));
   }
}

