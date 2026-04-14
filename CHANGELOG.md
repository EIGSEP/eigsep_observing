# Changelog

## [1.0.0](https://github.com/EIGSEP/eigsep_observing/compare/v0.0.1...v1.0.0) (2026-04-14)


### ⚠ BREAKING CHANGES

* use Redis-only pico interface via PicoManager
* absorb eigsep_corr into eigsep_observing
* HDF5 metadata key renames are downstream-visible. imu_panda -> imu_el (and the new imu_az key); temp_mon_a/_b removed; tempctrl_a/_b -> tempctrl_lna/tempctrl_load. Readers of pre-upgrade corr files that hardcode the old keys must be updated. read_hdf5 itself is unchanged and discovers metadata keys dynamically, so the file format is forward-compatible at the HDF5 layer.
* rename rfswitch sentinel from SWITCHING to UNKNOWN
* speed up filewriter with double-buffered async writes, iterative gap-fill, atomic I/O
* remove Redis control protocol (stream:ctrl) ([#23](https://github.com/EIGSEP/eigsep_observing/issues/23))

### Features

* add --interactive flag to fpga_init.py for live FPGA access ([#28](https://github.com/EIGSEP/eigsep_observing/issues/28)) ([2679d38](https://github.com/EIGSEP/eigsep_observing/commit/2679d3864de7deadea0e48b533c86ad27fbf27d9))
* add --rms flag and update input labels for adc_snapshot ([f1d286b](https://github.com/EIGSEP/eigsep_observing/commit/f1d286ba04f8a521e5937c86debcdc50c4e15950))
* add adc_snapshot plotter ([4fd99f9](https://github.com/EIGSEP/eigsep_observing/commit/4fd99f9c0ab846c701276b8154fc7a55b3836c4b))
* add avg_even_odd to corr config and file header ([5a00e9b](https://github.com/EIGSEP/eigsep_observing/commit/5a00e9b663a85dee5a5216900aee90b12fe0300c))
* save correlation files as int32 instead of float64/complex128 ([ea83081](https://github.com/EIGSEP/eigsep_observing/commit/ea83081878b4d90f7d65af3c7985f5c2f0e481a9))
* speed up filewriter with double-buffered async writes, iterative gap-fill, atomic I/O ([00c5529](https://github.com/EIGSEP/eigsep_observing/commit/00c5529a9837805bb6f26df20c4514102dd8a7f8))
* use contract-based corr-write, add test to enforce compliance ([edfcaa3](https://github.com/EIGSEP/eigsep_observing/commit/edfcaa366cea522291b01b485a0a0a9ea435b8bb))


### Bug Fixes

* emit logs with appropriate log levels on hardware failure as per claude.md ([12d1ff2](https://github.com/EIGSEP/eigsep_observing/commit/12d1ff298f978b73b5775d5436576974ebf96612))
* make avg_metadata differentiate between input types ([24e4603](https://github.com/EIGSEP/eigsep_observing/commit/24e4603a19019c2488006840a11c127b4d4a94d8))
* preserve corr data on rename failure and flush in close() ([2b63e6c](https://github.com/EIGSEP/eigsep_observing/commit/2b63e6cd073d3310cd741dd5822263edaaced839))
* read_hdf5 reconstructs complex from int32 cross data ([d22069f](https://github.com/EIGSEP/eigsep_observing/commit/d22069f78c102deee05f0ce697f229337c93a08f))


### Documentation

* add desired live status features ([cd238e8](https://github.com/EIGSEP/eigsep_observing/commit/cd238e8c7a4f44cf6d786c6601c1264b61629adc))
* carry forward hardware-requirements.txt for casperfpga pin ([1c99f45](https://github.com/EIGSEP/eigsep_observing/commit/1c99f451a8d71d5e92355cd48f65523ae2218830))
* refresh stale post-migration references in comments ([c3dc260](https://github.com/EIGSEP/eigsep_observing/commit/c3dc260cc3f356feaaf101e8388a7c0e3bf84fe9))


### Code Refactoring

* absorb eigsep_corr into eigsep_observing ([9c47b88](https://github.com/EIGSEP/eigsep_observing/commit/9c47b88bef67aaf2ea6d2730d0844465571d1d5b))
* migrate to picohost 1.0.0 ([742644a](https://github.com/EIGSEP/eigsep_observing/commit/742644a2bc8a8a6a236e321fec33c8325075ce12))
* remove Redis control protocol (stream:ctrl) ([#23](https://github.com/EIGSEP/eigsep_observing/issues/23)) ([53139bb](https://github.com/EIGSEP/eigsep_observing/commit/53139bbd57198f0528489ad3ed127edb827f0e0e))
* rename rfswitch sentinel from SWITCHING to UNKNOWN ([84826c4](https://github.com/EIGSEP/eigsep_observing/commit/84826c45deb90847bec834f656bed38c37e09021))
* use Redis-only pico interface via PicoManager ([4444ed0](https://github.com/EIGSEP/eigsep_observing/commit/4444ed033a1c21a3f0adfc3cb9e5373f50e148f1))
