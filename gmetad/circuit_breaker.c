/*
 * circuit_breaker.c
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <apr_time.h>

#include "gmetad.h"

extern gmetad_config_t gmetad_config;

extern g_tcp_socket *riemann_tcp_socket;

extern int riemann_circuit_breaker;
extern apr_time_t riemann_reset_timeout;
extern int riemann_failures;

extern g_tcp_socket* init_riemann_tcp_socket (const char *hostname, uint16_t port);

extern pthread_mutex_t  riemann_mutex;

/* Interval (seconds) between runs */
#define CIRCUIT_BREAKER_INTERVAL 10

void *
circuit_breaker_thread(void *arg)
{
   for (;;) {

      if (riemann_circuit_breaker == RIEMANN_CB_OPEN && riemann_reset_timeout < apr_time_now ()) {

         pthread_mutex_lock( &riemann_mutex );

         debug_msg ("[riemann] Reset period expired, retry connection...");
         riemann_circuit_breaker = RIEMANN_CB_HALF_OPEN;

         riemann_tcp_socket = init_riemann_tcp_socket (gmetad_config.riemann_server, gmetad_config.riemann_port);

         if (riemann_tcp_socket == NULL) {
            riemann_circuit_breaker = RIEMANN_CB_OPEN;
            riemann_reset_timeout = apr_time_now () + RIEMANN_RETRY_TIMEOUT * APR_USEC_PER_SEC;
         } else {
            riemann_circuit_breaker = RIEMANN_CB_CLOSED;
            riemann_failures = 0;
         }
         pthread_mutex_unlock( &riemann_mutex );
      }

      debug_msg("[riemann] circuit breaker is %s (%d)",
            riemann_circuit_breaker == RIEMANN_CB_CLOSED ? "CLOSED" :
            riemann_circuit_breaker == RIEMANN_CB_OPEN ?   "OPEN"
                              /* RIEMANN_CB_HALF_OPEN */ : "HALF_OPEN",
            riemann_circuit_breaker);

      apr_sleep(apr_time_from_sec(CIRCUIT_BREAKER_INTERVAL));
   }
}

