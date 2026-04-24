# Changelog

## [2.0.0](https://github.com/EIGSEP/eigsep_observing/compare/v1.0.0...v2.0.0) (2026-04-24)


### ⚠ BREAKING CHANGES

* split EigsepRedis into Transport + writer/reader classes
* callers using corr/VNA methods must switch from EigsepRedis to EigsepObsRedis. eigsep_observing.EigsepRedis now resolves to the base bus class (no corr/VNA methods).

### Features

* **corr:** gate CorrWriter.add on sync_time ([#47](https://github.com/EIGSEP/eigsep_observing/issues/47)) ([a189f0e](https://github.com/EIGSEP/eigsep_observing/commit/a189f0edc31599cecd4ddbac537997a8c3353ca7))
* **corr:** stamp header_upload_unix, publish on every state change ([#51](https://github.com/EIGSEP/eigsep_observing/issues/51)) ([757b541](https://github.com/EIGSEP/eigsep_observing/commit/757b5416c99ca794ff0af27d1b0089f503735742))
* **dev:** local fake-observation harness sharing one Redis ([#79](https://github.com/EIGSEP/eigsep_observing/issues/79)) ([11b2307](https://github.com/EIGSEP/eigsep_observing/commit/11b23073af310d22b6ef08e9cb6413e36867d1ef))
* **fpga-init:** close attach-path validation gap via Redis-as-truth ([e95d89a](https://github.com/EIGSEP/eigsep_observing/commit/e95d89a7bc063263aaea806569c0de4d3280915b))
* **fpga-init:** collapse init flags into --reinit, rehydrate sync_time on attach ([17a0bef](https://github.com/EIGSEP/eigsep_observing/commit/17a0befad2c4fcbf7e64b0fa05b191314230867e))
* **fpga:** publish ADC diagnostics to Redis ([b8fe8ec](https://github.com/EIGSEP/eigsep_observing/commit/b8fe8ec773c9e7c348d9cec27a3495f853fd8b68))
* **live-status:** add local Flask dashboard for field deployments ([83eb3e5](https://github.com/EIGSEP/eigsep_observing/commit/83eb3e5a13b8577237bcb44cd6f4ac30c65dc6c6))
* **motor:** migrate scan + manual scripts from picohost via PicoProxy ([#71](https://github.com/EIGSEP/eigsep_observing/issues/71)) ([287919f](https://github.com/EIGSEP/eigsep_observing/commit/287919fdeb9ce33234d5079ba935e8b1ef43c6d5))
* **observer:** split corr header-fetch failure modes with watchdog ([#48](https://github.com/EIGSEP/eigsep_observing/issues/48)) ([b17ae4d](https://github.com/EIGSEP/eigsep_observing/commit/b17ae4d9882b2ba527ba859fb0e6688bbd94f6c6))
* **observer:** unify SNAP liveness under a single deadline in record_corr_data ([8924a99](https://github.com/EIGSEP/eigsep_observing/commit/8924a999d9b8656ba0334b02dae548f15da4b528))
* **panda-client:** add switch_session context manager ([#59](https://github.com/EIGSEP/eigsep_observing/issues/59)) ([bddbb32](https://github.com/EIGSEP/eigsep_observing/commit/bddbb32553addbe974a801d46c9e34231cc218fc))
* **panda-client:** bump status stream maxlen 5 → 100 ([51aafcb](https://github.com/EIGSEP/eigsep_observing/commit/51aafcb10114256c9bc1d72c53ef97776a283e62))
* **panda-client:** route panda warnings to ground via status stream; fix logger setup ([ccae27b](https://github.com/EIGSEP/eigsep_observing/commit/ccae27bad8fea604e96b707a2a5661a434f3a06a))
* **panda-client:** self-validate VNA S11 payload; emit violations to ground via status stream ([75e129a](https://github.com/EIGSEP/eigsep_observing/commit/75e129ad89532e55efc5086cc39506afe14c11f7))
* **panda:** add motor_loop + use_motor gating to PandaClient ([#72](https://github.com/EIGSEP/eigsep_observing/issues/72)) ([b03c54c](https://github.com/EIGSEP/eigsep_observing/commit/b03c54c16a4b5935c19a97dff0594aa4afb66871))
* **panda:** add tempctrl_loop + use_tempctrl gating to PandaClient ([#74](https://github.com/EIGSEP/eigsep_observing/issues/74)) ([58cad56](https://github.com/EIGSEP/eigsep_observing/commit/58cad5685d0d7634fae16125165b969b1ed25249))
* **panda:** enforce boot-time RFANT + vna_loop exception recovery ([#70](https://github.com/EIGSEP/eigsep_observing/issues/70)) ([154cf8b](https://github.com/EIGSEP/eigsep_observing/commit/154cf8ba94614c851974e800c6e3b6218fc5f679))
* **redis:** warn on corr stream gaps via acc_cnt monotonicity ([2b8279d](https://github.com/EIGSEP/eigsep_observing/commit/2b8279d79e4fb7792e129ec199a50e72a9ddb496))
* **redis:** warn on stale metadata snapshot reads ([#62](https://github.com/EIGSEP/eigsep_observing/issues/62)) ([91b855c](https://github.com/EIGSEP/eigsep_observing/commit/91b855cea228bb8e9ebe3f3666b2cd541e73a03a))
* **redis:** warn on stale metadata stream drains ([87da4d1](https://github.com/EIGSEP/eigsep_observing/commit/87da4d1067c8fa5f8abe24e11338f0d3b9aff22e))


### Bug Fixes

* change config ownership to Panda ([38bee1e](https://github.com/EIGSEP/eigsep_observing/commit/38bee1e44a106f65a5e7f82b9294c23ef9a518ae))
* **panda-client:** drop dead VNA poll, stop mutating cfg, add stop() ([1e6fd56](https://github.com/EIGSEP/eigsep_observing/commit/1e6fd5646a002eee91a2973aa835af32334f9cf4))
* **panda-client:** warn on failed VNA switch-back ([b7f98a4](https://github.com/EIGSEP/eigsep_observing/commit/b7f98a4588ce94ab8bb761aae7cece66de8ad331))
* **panda:** survive proxy errors in RF switch observing loops ([0530d79](https://github.com/EIGSEP/eigsep_observing/commit/0530d7962deb93aa5197520308813ead57820780))
* restrict add_metadata to json serializable objects only ([52d488b](https://github.com/EIGSEP/eigsep_observing/commit/52d488bd3a36e6b2c4aa8d861d71575279bb255b))
* seperate metadata streams from data streams in EigRedis class ([b8cc1ed](https://github.com/EIGSEP/eigsep_observing/commit/b8cc1ed575a500f057fa1cc653beee1a3d45d1d3))


### Documentation

* make inline comment about corr watchdog less verbose ([67a037d](https://github.com/EIGSEP/eigsep_observing/commit/67a037d6f1dff3b64656ca594be0592385e0c0c9))


### Code Refactoring

* split bus primitives into shared eigsep_redis package ([d898ef6](https://github.com/EIGSEP/eigsep_observing/commit/d898ef6013f6037d4ffd4235a55332488f711557))
* split EigsepRedis into Transport + writer/reader classes ([693d73d](https://github.com/EIGSEP/eigsep_observing/commit/693d73db9d83e7bf8b3a7002241d4ed9704651c9))

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
