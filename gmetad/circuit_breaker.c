/*
 * circuit_breaker.c
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <apr_time.h>

#include "gmetad.h"
#include "riemann.pb-c.h"

extern gmetad_config_t gmetad_config;

extern g_tcp_socket *riemann_tcp_socket;

extern int riemann_circuit_breaker;
extern apr_time_t riemann_reset_timeout;
extern int riemann_failures;

extern g_tcp_socket* init_riemann_tcp_socket (const char *hostname, uint16_t port);

extern pthread_mutex_t  riemann_mutex;

void *
circuit_breaker_thread(void *arg)
{
   for (;;) {

      if (riemann_circuit_breaker == RIEMANN_CB_OPEN && riemann_reset_timeout < apr_time_now ()) {

         debug_msg ("[riemann] Reset period expired, retry connection...");
         pthread_mutex_lock( &riemann_mutex );
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

      if (riemann_circuit_breaker == RIEMANN_CB_CLOSED) {

         Msg *response;
         uint32_t len;
         ssize_t nbytes;
         uint32_t header;
         uint8_t *rbuf;

         debug_msg("[riemann] Waiting for a response...");
         pthread_mutex_lock( &riemann_mutex );
         nbytes = recv (riemann_tcp_socket->sockfd, &header, sizeof (header), 0);
         pthread_mutex_unlock( &riemann_mutex );

         if (nbytes == 0) {  /* server closed connection */
            err_msg ("[riemann] server closed connection");
            pthread_mutex_lock( &riemann_mutex );
            riemann_circuit_breaker = RIEMANN_CB_OPEN;
            pthread_mutex_unlock( &riemann_mutex );
         } else if (nbytes == -1) {
            err_msg ("[riemann] %s", strerror(errno));
            pthread_mutex_lock( &riemann_mutex );
            debug_msg("[riemann] recv() timeout ??");
            // riemann_failures++;
            pthread_mutex_unlock( &riemann_mutex );
         } else if (nbytes != sizeof (header)) {
            err_msg ("[riemann] error occurred receiving response");
            pthread_mutex_lock( &riemann_mutex );
            riemann_failures++;
            pthread_mutex_unlock( &riemann_mutex );
         } else {
            len = ntohl (header);
            rbuf = malloc (len);
            pthread_mutex_lock( &riemann_mutex );
            nbytes = recv (riemann_tcp_socket->sockfd, rbuf, len, 0);
            pthread_mutex_unlock( &riemann_mutex );

            if (nbytes == 0) {
               debug_msg("[riemann] response failed - server closed connection");
               /* server closed connection */
            } else if (nbytes == -1) {
               debug_msg("[riemann] response failed - general error");
               /* error */
            } else {
               response = msg__unpack (NULL, len, rbuf);
               debug_msg ("[riemann] message response ok=%d", response->ok);
               free (rbuf);

               if (response->ok != 1) {
                  debug_msg("[riemann] NOT OK");
                  if (response->error)
                     err_msg("[riemann] Reponse error: %s", response->error);
                  pthread_mutex_lock( &riemann_mutex );
                  riemann_failures++;
                  pthread_mutex_unlock( &riemann_mutex );
               } else {
                  debug_msg("[riemann] Received OK");
               }
            }
         }
      }
      if (riemann_failures > RIEMANN_MAX_FAILURES) {
         err_msg("[riemann] %d send/recv failures (> %d max.) - OPEN circuit breaker for %d seconds...",
               riemann_failures, RIEMANN_MAX_FAILURES, RIEMANN_RETRY_TIMEOUT);
         pthread_mutex_lock( &riemann_mutex );
         riemann_circuit_breaker = RIEMANN_CB_OPEN;
         riemann_reset_timeout = apr_time_now () + RIEMANN_RETRY_TIMEOUT * APR_USEC_PER_SEC;
         pthread_mutex_unlock( &riemann_mutex );
      }

      debug_msg("[riemann] circuit breaker is %s (%d)",
            riemann_circuit_breaker == RIEMANN_CB_CLOSED ? "CLOSED" :
            riemann_circuit_breaker == RIEMANN_CB_OPEN ?   "OPEN"
                              /* RIEMANN_CB_HALF_OPEN */ : "HALF_OPEN",
            riemann_circuit_breaker);
   }
}

