/**
 * Copyright 2013-2023 Software Radio Systems Limited
 *
 * This file is part of srsRAN.
 *
 * srsRAN is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * srsRAN is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * A copy of the GNU Affero General Public License can be found in
 * the LICENSE file in the top-level directory of this distribution
 * and at http://www.gnu.org/licenses/.
 *
 */

#include <libbladeRF.h>
#include <string.h>
#include <unistd.h>

#include "rf_blade_imp.h"
#include "rf_helper.h"
#include "rf_plugin.h"
#include "srsran/phy/common/timestamp.h"
#include "srsran/phy/utils/debug.h"
#include "srsran/phy/utils/vector.h"

#define UNUSED __attribute__((unused))
#define CONVERT_BUFFER_SIZE (128 * 1024 * sizeof(int16_t))

const unsigned int num_buffers   = 256;
const unsigned int num_transfers = 64;
const unsigned int timeout_ms    = 1000;

typedef struct {
  struct bladerf* dev;

  uint32_t nof_tx_channels;
  uint32_t nof_rx_channels;

  bladerf_sample_rate tx_rate;
  bladerf_sample_rate rx_rate;

  float          iq_scale;
  size_t         sample_size;
  bladerf_format format;
  bladerf_format buffer_format;

  int8_t rx_buffer[CONVERT_BUFFER_SIZE];
  int8_t tx_buffer[CONVERT_BUFFER_SIZE];

  bool rx_stream_enabled;
  bool tx_stream_enabled;

  srsran_rf_info_t info;
} rf_blade_handler_t;

static srsran_rf_error_handler_t blade_error_handler     = NULL;
static void*                     blade_error_handler_arg = NULL;

void rf_blade_suppress_stdout(UNUSED void* h) {}

void rf_blade_register_error_handler(UNUSED void* ptr, srsran_rf_error_handler_t new_handler, void* arg)
{
  blade_error_handler     = new_handler;
  blade_error_handler_arg = arg;
}

const char* rf_blade_devname(UNUSED void* h)
{
  return DEVNAME;
}

int rf_blade_start_tx_stream(void* h)
{
  int                 status;
  rf_blade_handler_t* handler = (rf_blade_handler_t*)h;

  const unsigned int buffer_size = 2048 + 1024 * (int)(handler->tx_rate / 1e7);

  printf("Starting Tx stream with %u channels, %zu-bit samples at %.2f MHz and %u samples per buffer...\n",
         handler->nof_tx_channels,
         handler->sample_size * 8,
         handler->tx_rate / 1e6,
         buffer_size);

  /* Configure the device's TX module for use with the sync interface.
   * SC16 Q11 or SC8 Q7 samples *with* metadata are used. */
  status = bladerf_sync_config(handler->dev,
                               handler->nof_tx_channels == 1 ? BLADERF_TX_X1 : BLADERF_TX_X2,
                               handler->format,
                               num_buffers,
                               buffer_size,
                               num_transfers,
                               timeout_ms);
  if (status != 0) {
    ERROR("Failed to configure TX sync interface: %s", bladerf_strerror(status));
    return status;
  }
  printf("Enabling Tx module for channel 1...\n");
  status = bladerf_enable_module(handler->dev, BLADERF_TX_X1, true);
  if (status != 0) {
    ERROR("Failed to enable TX module: %s", bladerf_strerror(status));
    return status;
  }
  if (handler->nof_tx_channels > 1) {
    printf("Enabling Rx module for channel 2...\n");
    status = bladerf_enable_module(handler->dev, BLADERF_TX_X2, true);
    if (status != 0) {
      ERROR("Failed to enable TX module for channel 2: %s", bladerf_strerror(status));
      return status;
    }
  }
  handler->tx_stream_enabled = true;
  return 0;
}

int rf_blade_start_rx_stream(void* h, UNUSED bool now)
{
  int                 status;
  rf_blade_handler_t* handler = (rf_blade_handler_t*)h;

  const unsigned int buffer_size = 2048 + 1024 * (int)(handler->rx_rate / 1e7);

  printf("Starting Rx stream with %u channels, %zu-bit samples at %.2f MHz and %u samples per buffer...\n",
         handler->nof_tx_channels,
         handler->sample_size * 8,
         handler->rx_rate / 1e6,
         buffer_size);

  /* Configure the device's RX module for use with the sync interface.
   * SC16 Q11 or SC8 Q7 samples *with* metadata are used. */
  status = bladerf_sync_config(handler->dev,
                               handler->nof_rx_channels == 1 ? BLADERF_RX_X1 : BLADERF_RX_X2,
                               handler->format,
                               num_buffers,
                               buffer_size,
                               num_transfers,
                               timeout_ms);
  if (status != 0) {
    ERROR("Failed to configure RX sync interface: %s", bladerf_strerror(status));
    return status;
  }

  printf("Enabling Rx module for channel 1...\n");
  status = bladerf_enable_module(handler->dev, BLADERF_RX_X1, true);
  if (status != 0) {
    ERROR("Failed to enable RX module: %s", bladerf_strerror(status));
    return status;
  }
  if (handler->nof_rx_channels > 1) {
    printf("Enabling Rx module for channel 2...\n");
    status = bladerf_enable_module(handler->dev, BLADERF_RX_X2, true);
    if (status != 0) {
      ERROR("Failed to enable RX module for channel 2: %s", bladerf_strerror(status));
      return status;
    }
  }
  handler->rx_stream_enabled = true;
  return 0;
}

int rf_blade_stop_rx_stream(void* h)
{
  rf_blade_handler_t* handler = (rf_blade_handler_t*)h;

  printf("Disabling Rx module for channel 1...\n");
  int status = bladerf_enable_module(handler->dev, BLADERF_RX_X1, false);
  if (status != 0) {
    ERROR("Failed to disable RX module: %s", bladerf_strerror(status));
    return status;
  }
  if (handler->nof_rx_channels > 1) {
    printf("Disabling Rx module for channel 2...\n");
    status = bladerf_enable_module(handler->dev, BLADERF_RX_X2, false);
    if (status != 0) {
      ERROR("Failed to disable RX module for channel 2: %s", bladerf_strerror(status));
      return status;
    }
  }
  printf("Disabling Tx module for channel 1...\n");
  status = bladerf_enable_module(handler->dev, BLADERF_TX_X1, false);
  if (status != 0) {
    ERROR("Failed to disable TX module: %s", bladerf_strerror(status));
    return status;
  }
  if (handler->nof_tx_channels > 1) {
    printf("Disabling Tx module for channel 2...\n");
    status = bladerf_enable_module(handler->dev, BLADERF_TX_X2, false);
    if (status != 0) {
      ERROR("Failed to disable TX module for channel 2: %s", bladerf_strerror(status));
      return status;
    }
  }
  handler->rx_stream_enabled = false;
  handler->tx_stream_enabled = false;
  return 0;
}

void rf_blade_flush_buffer(UNUSED void* h) {}

bool rf_blade_has_rssi(UNUSED void* h)
{
  return false;
}

float rf_blade_get_rssi(UNUSED void* h)
{
  return 0;
}

int rf_blade_open_multi(char* args, void** h, uint32_t nof_channels)
{
  *h = NULL;

  rf_blade_handler_t* handler = (rf_blade_handler_t*)malloc(sizeof(rf_blade_handler_t));
  if (!handler) {
    perror("malloc");
    return SRSRAN_ERROR;
  }
  *h = handler;

  if (nof_channels > 2) {
    ERROR("Invalid nof_channels %u, should be 1 or 2", nof_channels);
    return SRSRAN_ERROR;
  }

  handler->nof_tx_channels = 0;
  handler->nof_rx_channels = 0;

  parse_uint32(args, "nof_tx_channels", 0, &handler->nof_tx_channels);
  parse_uint32(args, "nof_rx_channels", 0, &handler->nof_rx_channels);

  if (handler->nof_tx_channels == 0 || handler->nof_tx_channels > nof_channels) {
    handler->nof_tx_channels = nof_channels;
  }
  if (handler->nof_rx_channels == 0 || handler->nof_rx_channels > nof_channels) {
    handler->nof_rx_channels = nof_channels;
  }

  char format[RF_PARAM_LEN] = "sc16";
  parse_string(args, "format", 0, format);

  if (strcmp(format, "sc16") == 0) {
    handler->iq_scale      = 2048.;
    handler->sample_size   = sizeof(int16_t);
    handler->format        = BLADERF_FORMAT_SC16_Q11_META;
    handler->buffer_format = BLADERF_FORMAT_SC16_Q11;
  } else if (strcmp(format, "sc8") == 0) {
    handler->iq_scale      = 128.;
    handler->sample_size   = sizeof(int8_t);
    handler->format        = BLADERF_FORMAT_SC8_Q7_META;
    handler->buffer_format = BLADERF_FORMAT_SC8_Q7;
  } else {
    ERROR("Invalid format %s, should be sc8 or sc16", format);
    return SRSRAN_ERROR;
  }

  char log_level[RF_PARAM_LEN] = "silent";
  parse_string(args, "log_level", 0, log_level);

  if (strcmp(log_level, "verbose") == 0) {
    bladerf_log_set_verbosity(BLADERF_LOG_LEVEL_VERBOSE);
  } else if (strcmp(log_level, "debug") == 0) {
    bladerf_log_set_verbosity(BLADERF_LOG_LEVEL_DEBUG);
  } else if (strcmp(log_level, "info") == 0) {
    bladerf_log_set_verbosity(BLADERF_LOG_LEVEL_INFO);
  } else if (strcmp(log_level, "warn") == 0) {
    bladerf_log_set_verbosity(BLADERF_LOG_LEVEL_WARNING);
  } else if (strcmp(log_level, "error") == 0) {
    bladerf_log_set_verbosity(BLADERF_LOG_LEVEL_ERROR);
  } else if (strcmp(log_level, "critical") == 0) {
    bladerf_log_set_verbosity(BLADERF_LOG_LEVEL_CRITICAL);
  } else if (strcmp(log_level, "silent") == 0) {
    bladerf_log_set_verbosity(BLADERF_LOG_LEVEL_SILENT);
  } else {
    ERROR("Invalid log_level %s, should be verbose, debug, info, warn, error, critical or silent", log_level);
    return SRSRAN_ERROR;
  }

  char device_id[RF_PARAM_LEN] = "";
  parse_string(args, "device_id", 0, device_id);

  char tuning_mode[RF_PARAM_LEN] = "host";
  parse_string(args, "tuning_mode", 0, tuning_mode);

  printf("Opening bladeRF...\n");
  int status = bladerf_open(&handler->dev, device_id);
  if (status) {
    ERROR("Unable to open device: %s", bladerf_strerror(status));
    goto clean_exit;
  }

  printf("Setting tuning mode...\n");

  if (strcmp(tuning_mode, "fpga") == 0) {
    status = bladerf_set_tuning_mode(handler->dev, BLADERF_TUNING_MODE_FPGA);
  } else if (strcmp(tuning_mode, "host") == 0) {
    status = bladerf_set_tuning_mode(handler->dev, BLADERF_TUNING_MODE_HOST);
  } else {
    ERROR("Invalid tuning_mode %s, should be host or fpga", tuning_mode);
    status = SRSRAN_ERROR;
    goto clean_exit;
  }

  if (status) {
    ERROR("Unable to set tuning mode: %s", bladerf_strerror(status));
    goto clean_exit;
  }

  printf("Setting manual gain...\n");

  status = bladerf_set_gain_mode(handler->dev, BLADERF_RX_X1, BLADERF_GAIN_MGC);
  if (status) {
    ERROR("Unable to set gain mode: %s", bladerf_strerror(status));
    goto clean_exit;
  }
  if (handler->nof_rx_channels > 1) {
    status = bladerf_set_gain_mode(handler->dev, BLADERF_RX_X2, BLADERF_GAIN_MGC);
    if (status) {
      ERROR("Unable to set gain mode for channel 2: %s", bladerf_strerror(status));
      goto clean_exit;
    }
  }

  handler->rx_stream_enabled = false;
  handler->tx_stream_enabled = false;

  return SRSRAN_SUCCESS;

clean_exit:
  free(handler);
  return status;
}

int rf_blade_open(char* args, void** h)
{
  return rf_blade_open_multi(args, h, 1);
}

int rf_blade_close(void* h)
{
  rf_blade_handler_t* handler = (rf_blade_handler_t*)h;
  printf("Closing bladeRF...\n");
  bladerf_close(handler->dev);
  return 0;
}

double rf_blade_set_rx_srate(void* h, double freq)
{
  uint32_t            bw;
  rf_blade_handler_t* handler = (rf_blade_handler_t*)h;
  int                 status  = bladerf_set_sample_rate(handler->dev, BLADERF_RX_X1, (uint32_t)freq, &handler->rx_rate);
  if (status != 0) {
    ERROR("Failed to set samplerate = %u: %s", (uint32_t)freq, bladerf_strerror(status));
    return -1;
  }

  status = bladerf_set_bandwidth(handler->dev, BLADERF_RX_X1, (bladerf_bandwidth)(handler->rx_rate * 0.9), &bw);
  if (status != 0) {
    ERROR("Failed to set bandwidth = %u: %s", handler->rx_rate, bladerf_strerror(status));
    return -1;
  }
  printf("Set RX sampling rate %.2f Mhz, filter BW: %.2f Mhz\n", (float)handler->rx_rate / 1e6, (float)bw / 1e6);
  return (double)handler->rx_rate;
}

double rf_blade_set_tx_srate(void* h, double freq)
{
  uint32_t            bw;
  rf_blade_handler_t* handler = (rf_blade_handler_t*)h;
  int                 status  = bladerf_set_sample_rate(handler->dev, BLADERF_TX_X1, (uint32_t)freq, &handler->tx_rate);
  if (status != 0) {
    ERROR("Failed to set samplerate = %u: %s", (uint32_t)freq, bladerf_strerror(status));
    return -1;
  }
  status = bladerf_set_bandwidth(handler->dev, BLADERF_TX_X1, handler->tx_rate, &bw);
  if (status != 0) {
    ERROR("Failed to set bandwidth = %u: %s", handler->tx_rate, bladerf_strerror(status));
    return -1;
  }
  printf("Set TX sampling rate %.2f Mhz, filter BW: %.2f Mhz\n", (float)handler->tx_rate / 1e6, (float)bw / 1e6);
  return (double)handler->tx_rate;
}

int rf_blade_set_rx_gain_ch(void* h, UNUSED uint32_t ch, double gain)
{
  rf_blade_handler_t* handler = (rf_blade_handler_t*)h;

  printf("Setting Rx gain for channel 1 to %.1f...\n", gain);
  int status = bladerf_set_gain(handler->dev, BLADERF_RX_X1, (bladerf_gain)gain);
  if (status != 0) {
    ERROR("Failed to set RX gain: %s", bladerf_strerror(status));
    return SRSRAN_ERROR;
  }
  if (handler->nof_rx_channels > 1) {
    printf("Setting Rx gain for channel 2 to %.1f...\n", gain);
    status = bladerf_set_gain(handler->dev, BLADERF_RX_X2, (bladerf_gain)gain);
    if (status != 0) {
      ERROR("Failed to set RX gain: %s", bladerf_strerror(status));
      return SRSRAN_ERROR;
    }
  }
  return SRSRAN_SUCCESS;
}

int rf_blade_set_rx_gain(void* h, double gain)
{
  return rf_blade_set_rx_gain_ch(h, 0, gain);
}

int rf_blade_set_tx_gain_ch(void* h, UNUSED uint32_t ch, double gain)
{
  rf_blade_handler_t* handler = (rf_blade_handler_t*)h;

  printf("Setting Tx gain for channel 1 to %.1f...\n", gain);
  int status = bladerf_set_gain(handler->dev, BLADERF_TX_X1, (bladerf_gain)gain);
  if (status != 0) {
    ERROR("Failed to set TX gain: %s", bladerf_strerror(status));
    return SRSRAN_ERROR;
  }
  if (handler->nof_tx_channels > 1) {
    printf("Setting Tx gain for channel 2 to %.1f...\n", gain);
    status = bladerf_set_gain(handler->dev, BLADERF_TX_X2, (bladerf_gain)gain);
    if (status != 0) {
      ERROR("Failed to set RX gain: %s", bladerf_strerror(status));
      return SRSRAN_ERROR;
    }
  }
  return SRSRAN_SUCCESS;
}

int rf_blade_set_tx_gain(void* h, double gain)
{
  return rf_blade_set_tx_gain_ch(h, 0, gain);
}

double rf_blade_get_rx_gain(void* h)
{
  int                 status;
  bladerf_gain        gain    = 0;
  rf_blade_handler_t* handler = (rf_blade_handler_t*)h;
  status                      = bladerf_get_gain(handler->dev, BLADERF_RX_X1, &gain);
  if (status != 0) {
    ERROR("Failed to get RX gain: %s", bladerf_strerror(status));
    return -1;
  }
  return gain;
}

double rf_blade_get_tx_gain(void* h)
{
  int                 status;
  bladerf_gain        gain    = 0;
  rf_blade_handler_t* handler = (rf_blade_handler_t*)h;
  status                      = bladerf_get_gain(handler->dev, BLADERF_TX_X1, &gain);
  if (status != 0) {
    ERROR("Failed to get TX gain: %s", bladerf_strerror(status));
    return -1;
  }
  return gain;
}

srsran_rf_info_t* rf_blade_get_info(void* h)
{
  srsran_rf_info_t* info = NULL;

  if (h) {
    rf_blade_handler_t* handler = (rf_blade_handler_t*)h;

    info = &handler->info;
  }
  return info;
}

double rf_blade_set_rx_freq(void* h, uint32_t ch, double freq)
{
  rf_blade_handler_t* handler = (rf_blade_handler_t*)h;
  bladerf_frequency   f_int   = (uint32_t)round(freq);
  int                 status  = bladerf_set_frequency(handler->dev, ch == 0 ? BLADERF_RX_X1 : BLADERF_RX_X2, f_int);
  if (status != 0) {
    ERROR("Failed to set samplerate = %u: %s", (uint32_t)freq, bladerf_strerror(status));
    return -1;
  }
  f_int = 0;
  bladerf_get_frequency(handler->dev, ch == 0 ? BLADERF_RX_X1 : BLADERF_RX_X2, &f_int);
  printf("Set RX frequency for channel %u to %lu\n", ch + 1, f_int);

  return freq;
}

double rf_blade_set_tx_freq(void* h, uint32_t ch, double freq)
{
  rf_blade_handler_t* handler = (rf_blade_handler_t*)h;
  bladerf_frequency   f_int   = (uint32_t)round(freq);
  int                 status  = bladerf_set_frequency(handler->dev, ch == 0 ? BLADERF_TX_X1 : BLADERF_TX_X2, f_int);
  if (status != 0) {
    ERROR("Failed to set samplerate = %u: %s", (uint32_t)freq, bladerf_strerror(status));
    return -1;
  }

  f_int = 0;
  bladerf_get_frequency(handler->dev, ch == 0 ? BLADERF_TX_X1 : BLADERF_TX_X2, &f_int);
  printf("Set TX frequency for channel %u to %lu\n", ch + 1, f_int);
  return freq;
}

static void timestamp_to_secs(uint32_t rate, uint64_t timestamp, time_t* secs, double* frac_secs)
{
  double totalsecs = (double)timestamp / rate;
  time_t secs_i    = (time_t)totalsecs;
  if (secs) {
    *secs = secs_i;
  }
  if (frac_secs) {
    *frac_secs = totalsecs - secs_i;
  }
}

void rf_blade_get_time(void* h, time_t* secs, double* frac_secs)
{
  rf_blade_handler_t*     handler = (rf_blade_handler_t*)h;
  struct bladerf_metadata meta;

  int status = bladerf_get_timestamp(handler->dev, BLADERF_RX, &meta.timestamp);
  if (status != 0) {
    ERROR("Failed to get current RX timestamp: %s", bladerf_strerror(status));
  }
  timestamp_to_secs(handler->rx_rate, meta.timestamp, secs, frac_secs);
}

int rf_blade_recv_with_time_multi(void*       h,
                                  void**      data,
                                  uint32_t    nsamples,
                                  UNUSED bool blocking,
                                  time_t*     secs,
                                  double*     frac_secs)
{
  rf_blade_handler_t*     handler = (rf_blade_handler_t*)h;
  struct bladerf_metadata meta;
  int                     status;

  memset(&meta, 0, sizeof(meta));
  meta.flags = BLADERF_META_FLAG_RX_NOW;

  if (2 * nsamples * handler->sample_size * handler->nof_rx_channels > CONVERT_BUFFER_SIZE) {
    unsigned int buffer_size = CONVERT_BUFFER_SIZE / 2 / handler->sample_size / handler->nof_rx_channels;
    ERROR("RX failed: nsamples exceeds buffer size (%u > %u)", nsamples, buffer_size);
    return -1;
  }
  status = bladerf_sync_rx(handler->dev, handler->rx_buffer, nsamples * handler->nof_rx_channels, &meta, timeout_ms);
  if (status) {
    ERROR("RX failed: %s; nsamples=%d;", bladerf_strerror(status), nsamples);
    return -1;
  } else if (meta.status & BLADERF_META_STATUS_OVERRUN) {
    if (blade_error_handler) {
      srsran_rf_error_t error;
      if (nsamples != meta.actual_count / handler->nof_rx_channels) {
        error.opt  = meta.actual_count;
        error.type = SRSRAN_RF_ERROR_OVERFLOW;
      } else {
        error.type = SRSRAN_RF_ERROR_UNDERFLOW;
      }
      blade_error_handler(blade_error_handler_arg, error);
    } else {
      /*ERROR("Overrun detected in scheduled RX. "
            "%u valid samples were read.", meta.actual_count);*/
    }
  }

  timestamp_to_secs(handler->rx_rate, meta.timestamp, secs, frac_secs);

  status = bladerf_deinterleave_stream_buffer(handler->nof_rx_channels == 1 ? BLADERF_RX_X1 : BLADERF_RX_X2,
                                              handler->buffer_format,
                                              meta.actual_count * handler->nof_rx_channels,
                                              handler->rx_buffer);
  if (status != 0) {
    ERROR("RX failed: could not interleave stream buffer: %s", bladerf_strerror(status));
    return -1;
  }

  nsamples = meta.actual_count / handler->nof_rx_channels;

  for (uint32_t i = 0; i < handler->nof_rx_channels; i++) {
    if (data[i] != NULL) {
      if (handler->buffer_format == BLADERF_FORMAT_SC8_Q7) {
        srsran_vec_convert_bf(handler->rx_buffer + 2 * nsamples * i, handler->iq_scale, data[i], 2 * nsamples);
      } else {
        const int16_t* rx_buffer = (const int16_t*)handler->rx_buffer;
        srsran_vec_convert_if(rx_buffer + 2 * nsamples * i, handler->iq_scale, data[i], 2 * nsamples);
      }
    }
  }

  return nsamples;
}

int rf_blade_recv_with_time(void* h, void* data, uint32_t nsamples, bool blocking, time_t* secs, double* frac_secs)
{
  void* datav[] = {data, NULL, NULL, NULL};
  return rf_blade_recv_with_time_multi(h, datav, nsamples, blocking, secs, frac_secs);
}

int rf_blade_send_timed_multi(void*       h,
                              void*       data[4],
                              int         nsamples,
                              time_t      secs,
                              double      frac_secs,
                              bool        has_time_spec,
                              UNUSED bool blocking,
                              bool        is_start_of_burst,
                              bool        is_end_of_burst)
{
  rf_blade_handler_t*     handler = (rf_blade_handler_t*)h;
  struct bladerf_metadata meta;
  int                     status;

  if (!handler->tx_stream_enabled) {
    rf_blade_start_tx_stream(h);
  }

  if (2 * nsamples * handler->sample_size * handler->nof_tx_channels > CONVERT_BUFFER_SIZE) {
    unsigned int buffer_size = CONVERT_BUFFER_SIZE / 2 / handler->sample_size / handler->nof_tx_channels;
    ERROR("TX failed: nsamples exceeds buffer size (%d > %d)", nsamples, buffer_size);
    return -1;
  }

  for (uint32_t i = 0; i < handler->nof_tx_channels; i++) {
    if (data[i] != NULL) {
      if (handler->buffer_format == BLADERF_FORMAT_SC8_Q7) {
        srsran_vec_convert_fb(data[i], handler->iq_scale, handler->tx_buffer + 2 * nsamples * i, 2 * nsamples);
      } else {
        int16_t* tx_buffer = (int16_t*)handler->tx_buffer;
        srsran_vec_convert_fi(data[i], handler->iq_scale, tx_buffer + 2 * nsamples * i, 2 * nsamples);
      }
    } else {
      memset(handler->tx_buffer + 2 * nsamples * i * handler->sample_size, 0, 2 * nsamples * handler->sample_size);
    }
  }

  status = bladerf_interleave_stream_buffer(handler->nof_tx_channels == 1 ? BLADERF_TX_X1 : BLADERF_TX_X2,
                                            handler->buffer_format,
                                            nsamples * handler->nof_tx_channels,
                                            handler->tx_buffer);
  if (status != 0) {
    ERROR("TX failed: could not interleave stream buffer: %s", bladerf_strerror(status));
    return -1;
  }

  memset(&meta, 0, sizeof(meta));
  if (is_start_of_burst) {
    if (has_time_spec) {
      // Convert time to ticks
      srsran_timestamp_t ts = {.full_secs = secs, .frac_secs = frac_secs};
      meta.timestamp        = srsran_timestamp_uint64(&ts, handler->tx_rate);
    } else {
      meta.flags |= BLADERF_META_FLAG_TX_NOW;
    }
    meta.flags |= BLADERF_META_FLAG_TX_BURST_START;
  }
  if (is_end_of_burst) {
    meta.flags |= BLADERF_META_FLAG_TX_BURST_END;
  }
  srsran_rf_error_t error;
  bzero(&error, sizeof(srsran_rf_error_t));

  status = bladerf_sync_tx(handler->dev, handler->tx_buffer, nsamples * handler->nof_tx_channels, &meta, timeout_ms);
  if (status == BLADERF_ERR_TIME_PAST) {
    if (blade_error_handler) {
      error.type = SRSRAN_RF_ERROR_LATE;
      blade_error_handler(blade_error_handler_arg, error);
    } else {
      ERROR("TX failed: %s", bladerf_strerror(status));
    }
  } else if (status) {
    ERROR("TX failed: %s", bladerf_strerror(status));
    return status;
  } else if (meta.status == BLADERF_META_STATUS_UNDERRUN) {
    if (blade_error_handler) {
      error.type = SRSRAN_RF_ERROR_UNDERFLOW;
      blade_error_handler(blade_error_handler_arg, error);
    } else {
      ERROR("TX warning: underflow detected.");
    }
  }

  return nsamples;
}

int rf_blade_send_timed(void*  h,
                        void*  data,
                        int    nsamples,
                        time_t secs,
                        double frac_secs,
                        bool   has_time_spec,
                        bool   blocking,
                        bool   is_start_of_burst,
                        bool   is_end_of_burst)
{
  void* datav[] = {data, NULL, NULL, NULL};
  return rf_blade_send_timed_multi(
      h, datav, nsamples, secs, frac_secs, has_time_spec, blocking, is_start_of_burst, is_end_of_burst);
}

rf_dev_t srsran_rf_dev_blade = {"bladeRF",
                                rf_blade_devname,
                                rf_blade_start_rx_stream,
                                rf_blade_stop_rx_stream,
                                rf_blade_flush_buffer,
                                rf_blade_has_rssi,
                                rf_blade_get_rssi,
                                rf_blade_suppress_stdout,
                                rf_blade_register_error_handler,
                                rf_blade_open,
                                .srsran_rf_open_multi = rf_blade_open_multi,
                                rf_blade_close,
                                rf_blade_set_rx_srate,
                                rf_blade_set_rx_gain,
                                rf_blade_set_rx_gain_ch,
                                rf_blade_set_tx_gain,
                                rf_blade_set_tx_gain_ch,
                                rf_blade_get_rx_gain,
                                rf_blade_get_tx_gain,
                                rf_blade_get_info,
                                rf_blade_set_rx_freq,
                                rf_blade_set_tx_srate,
                                rf_blade_set_tx_freq,
                                rf_blade_get_time,
                                NULL,
                                rf_blade_recv_with_time,
                                rf_blade_recv_with_time_multi,
                                rf_blade_send_timed,
                                .srsran_rf_send_timed_multi = rf_blade_send_timed_multi};

#ifdef ENABLE_RF_PLUGINS
int register_plugin(rf_dev_t** rf_api)
{
  if (rf_api == NULL) {
    return SRSRAN_ERROR;
  }
  *rf_api = &srsran_rf_dev_blade;
  return SRSRAN_SUCCESS;
}
#endif /* ENABLE_RF_PLUGINS */
