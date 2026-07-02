# Changelog

## [2.9.0](https://github.com/EIGSEP/eigsep_observing/compare/v2.8.0...v2.9.0) (2026-07-02)


### Features

* **io:** embed IMU mount calibration in corr + VNA file headers ([#176](https://github.com/EIGSEP/eigsep_observing/issues/176)) ([#177](https://github.com/EIGSEP/eigsep_observing/issues/177)) ([278688c](https://github.com/EIGSEP/eigsep_observing/commit/278688cd104aab821994f9ce312340e33d6984e1))
* **io:** record system_current cal params in metadata (picohost 3.11) ([#178](https://github.com/EIGSEP/eigsep_observing/issues/178)) ([9a91365](https://github.com/EIGSEP/eigsep_observing/commit/9a913655e5102528e8f91ef8364d1e1b3552ea24))
* **io:** split _IMU_SCHEMA into el/az schemas for calibrate-imu fields ([#172](https://github.com/EIGSEP/eigsep_observing/issues/172)) ([5674e11](https://github.com/EIGSEP/eigsep_observing/commit/5674e110f7ac0fb9286210c4a1bd7b9bb7b19942))
* **live_status:** Antenna pointing panel with sensor-spread drift/stall alarm ([#174](https://github.com/EIGSEP/eigsep_observing/issues/174)) ([d67738b](https://github.com/EIGSEP/eigsep_observing/commit/d67738b6d397b45003967512a4ecb9a6b453f4ab))


### Bug Fixes

* **observer:** downgrade expected obs_config overlay gap to WARNING ([af31343](https://github.com/EIGSEP/eigsep_observing/commit/af313434bd2d027e0d0e6d61e4db590657b30c1f))


### Documentation

* field calibration & zeroing runbook (lab/field/routine/recovery) ([f3832fb](https://github.com/EIGSEP/eigsep_observing/commit/f3832fbbfe0aae23d41ffe03c86e2984b8404494))
* **notebooks:** azimuth sensor cross-check + field-debug notebook ([20c1461](https://github.com/EIGSEP/eigsep_observing/commit/20c1461bcb87bb9972f0ba9c81961562e5acaa39))

## [2.8.0](https://github.com/EIGSEP/eigsep_observing/compare/v2.7.0...v2.8.0) (2026-06-28)


### Features

* **fpga:** support v2.4 firmware (single-spectrum), auto-detected ([0a6c9a3](https://github.com/EIGSEP/eigsep_observing/commit/0a6c9a367be669ce050f91c46196942d9fcf89e0))
* set adc_mux_sel register and record mapping ([abe3247](https://github.com/EIGSEP/eigsep_observing/commit/abe3247e360f287261ef73ea074d735b20ea0776))
* wire system_current into schemas, live status, and watch_sensors ([#169](https://github.com/EIGSEP/eigsep_observing/issues/169)) ([5707e9e](https://github.com/EIGSEP/eigsep_observing/commit/5707e9e4a9b8bddb681532f755e0101e5c54822a))


### Bug Fixes

* **watch_sensors:** drop pot_el_angle from potmon plot fields ([afee564](https://github.com/EIGSEP/eigsep_observing/commit/afee5644f3f59dacfc6e07da2a63c6b3e02b9b64))

## [2.7.0](https://github.com/EIGSEP/eigsep_observing/compare/v2.6.0...v2.7.0) (2026-06-20)


### Features

* **potmon:** drop el channel, az only ([9f162f4](https://github.com/EIGSEP/eigsep_observing/commit/9f162f441d4eb93a5374dd8199a9864cc2e4dc75))
* **scripts:** add watch_sensors, --no-save on record_metadata, drop monitor_meta ([#161](https://github.com/EIGSEP/eigsep_observing/issues/161)) ([643917e](https://github.com/EIGSEP/eigsep_observing/commit/643917e7d8e1bf7be23ee56b633f3d1f2571f71d))

## [2.6.0](https://github.com/EIGSEP/eigsep_observing/compare/v2.5.0...v2.6.0) (2026-06-19)


### Features

* **corr:** surface corr-loop health (dropped integrations + readout time) ([fcfd16b](https://github.com/EIGSEP/eigsep_observing/commit/fcfd16b82438ea8cd888090178f998577b19027f))
* **imu_manual:** add --plot live yaw/pitch/roll window ([be8853e](https://github.com/EIGSEP/eigsep_observing/commit/be8853e4901510a486c0ca3784beaf3d0bafa057))
* **imu_manual:** add --redis-host/--redis-port for remote readout ([cc71e94](https://github.com/EIGSEP/eigsep_observing/commit/cc71e94575eae6db818ace0c0c937cf425ae839f))
* **io:** add motor boot_id to schema and invariant fields ([5ce72f8](https://github.com/EIGSEP/eigsep_observing/commit/5ce72f83b799608bb44dd911f04be86aaae2e52e))
* **io:** add tempctrl voltage/resistance to _PELTIER_SCHEMA ([ea7f20a](https://github.com/EIGSEP/eigsep_observing/commit/ea7f20a58b6756f63f043855bc233f3debb258e6))
* **io:** add write/read_metadata_hdf5; align recorder with corr metadata format ([3076e0f](https://github.com/EIGSEP/eigsep_observing/commit/3076e0f2be775853d96fec40352b1f41c72394f0))
* **live_status:** matplotlib-style tick density on corr plot zoom ([7f2382b](https://github.com/EIGSEP/eigsep_observing/commit/7f2382b26d47554d4c596ffeeb0258fb8894e22b))
* **live-status:** flag stale picos with an amber tile and age ([e796c15](https://github.com/EIGSEP/eigsep_observing/commit/e796c15eb696ff1bcc0f3f4c5569c5c69d45367c))
* **live-status:** sci-notation y-axis, visible DC bin, 25 MHz ticks to 250 ([6a169c9](https://github.com/EIGSEP/eigsep_observing/commit/6a169c91179268200085f456eda2cac7e751c998))
* **live-status:** sunlight-readable themes (Sun/Light/Dark toggle) ([4f7cd9b](https://github.com/EIGSEP/eigsep_observing/commit/4f7cd9b35dd10adf4d51619339808413faa1d566))
* **motor-control:** configurable per-axis scan grid via CLI ([9286fba](https://github.com/EIGSEP/eigsep_observing/commit/9286fbaaa797ebae7147870a914e03c4d61c300e))
* **motor-manual:** require 'y' confirmation before zeroing ([c196cfa](https://github.com/EIGSEP/eigsep_observing/commit/c196cfaea23b8f58575d036b5bda57015b032650))
* **motor-manual:** show axis degrees alongside raw step counts ([#138](https://github.com/EIGSEP/eigsep_observing/issues/138)) ([c844d3b](https://github.com/EIGSEP/eigsep_observing/commit/c844d3b78df8005e9b2ed7d34f2766d3b06127b7))
* **motor:** go-home control in motor_manual + Ctrl-C home prompt in scan ([08ce226](https://github.com/EIGSEP/eigsep_observing/commit/08ce22672ef6d81c79533da9fc995b18d5c40356))
* **run_tag:** active/passive driver split + manual-session metadata completeness ([#157](https://github.com/EIGSEP/eigsep_observing/issues/157)) ([f2e5340](https://github.com/EIGSEP/eigsep_observing/commit/f2e5340523c6d8b459531d214a2e6dea41efce26))
* **run_tag:** auto-reclaim stale locks from provably-dead holders ([024e7db](https://github.com/EIGSEP/eigsep_observing/commit/024e7db00ec501f1586077713a201c0b9dc25ff9))
* **scripts:** add clear_run_tag.py recovery tool for stale locks ([f6647a6](https://github.com/EIGSEP/eigsep_observing/commit/f6647a69335678b854f3b4c746fac49945b77710))
* **tempctrl_manual:** step clamp up/down with c/C instead of cycling ([#151](https://github.com/EIGSEP/eigsep_observing/issues/151)) ([24f3b55](https://github.com/EIGSEP/eigsep_observing/commit/24f3b5566bba742664ae56d547f9ba656373a30e))
* **tempctrl-manual:** add `p` hotkey to plot temperature vs time ([c5f2af8](https://github.com/EIGSEP/eigsep_observing/commit/c5f2af8316747601415d3f8af2c779cea1e0dfc0))
* **tempctrl:** wire cooling_enabled runaway guard into config + manual script ([63b760d](https://github.com/EIGSEP/eigsep_observing/commit/63b760df46b4475bee85492b87941eb2d6b545f8))


### Bug Fixes

* changed argument pairs type int to str ([df0855e](https://github.com/EIGSEP/eigsep_observing/commit/df0855e87f8feb9dc41bf3831b067630e2451a3c))
* don't claim run tag for imu manual ([cb553f4](https://github.com/EIGSEP/eigsep_observing/commit/cb553f4a33a56b0521c52c30bdcda844cf364c50))
* **io:** field-aware invariant-disagreement log for boot_id ([e9b107d](https://github.com/EIGSEP/eigsep_observing/commit/e9b107d4430cb83f5d94051d724c9cfb0ee01b51))
* **live_status:** bound transport connect timeout so Ctrl-C is snappy ([064de1d](https://github.com/EIGSEP/eigsep_observing/commit/064de1d7244a1be837bcd4ab6d047ced2994de43))
* **live_status:** drain-time age semantics + poll-don't-wait ticks ([bc141b3](https://github.com/EIGSEP/eigsep_observing/commit/bc141b37638415312bb163706e3b0fa8dbea4868))
* **live-status:** persist corr legend toggles across integrations ([4ea26c7](https://github.com/EIGSEP/eigsep_observing/commit/4ea26c75d736fb411c2e9153d835e21c440d6e06))
* **motor:** serialize axis moves so home/jog never drive both motors ([5821e81](https://github.com/EIGSEP/eigsep_observing/commit/5821e8176052f9aacd33f0c0365760f9687afc99))
* **run_tag:** route _holder_is_dead through shared read_json helper ([38c3ef8](https://github.com/EIGSEP/eigsep_observing/commit/38c3ef8612d3ed41e65b9f25df457cebdb9c6fbe))

## [2.5.0](https://github.com/EIGSEP/eigsep_observing/compare/v2.4.0...v2.5.0) (2026-05-23)


### Features

* **client:** return published payload from measure_s11 ([2a27641](https://github.com/EIGSEP/eigsep_observing/commit/2a276411c3ff514c2a6d53d89fd5188fe4c57355))
* **live status:** pane redesign ([#118](https://github.com/EIGSEP/eigsep_observing/issues/118)) ([4909c4b](https://github.com/EIGSEP/eigsep_observing/commit/4909c4b94ad4dbeca76f2c4cc7f1de6eef354842))
* **observe:** tolerate panda outages without crashing the corr loop ([#130](https://github.com/EIGSEP/eigsep_observing/issues/130)) ([2416bcc](https://github.com/EIGSEP/eigsep_observing/commit/2416bcc103c497cbef595ceb12a44e96870bbbfb))
* **provenance:** gate obs_config uploads + run_tag every driver ([#132](https://github.com/EIGSEP/eigsep_observing/issues/132)) ([d5ba55e](https://github.com/EIGSEP/eigsep_observing/commit/d5ba55ea21356e7e82952eb7329d3c557e8c9af9))
* **scripts:** add record_metadata and record_vna for test-bench data collection ([09cd1b1](https://github.com/EIGSEP/eigsep_observing/commit/09cd1b1694cd45692e0d461350b9f30ec113282d))
* **scripts:** add vna_manual bring-up tool ([f4c7246](https://github.com/EIGSEP/eigsep_observing/commit/f4c7246aa3679cc431989e3caa08013407f858fd))
* **scripts:** promote alt-mode observers to console entry points ([0f7af99](https://github.com/EIGSEP/eigsep_observing/commit/0f7af99100406030c4e7d3751345e7134bfe8ec4))
* **tempctrl:** expose cooling-mode guard through TempCtrlClient ([3d54b4e](https://github.com/EIGSEP/eigsep_observing/commit/3d54b4eb86fafac5ace4518c36d215ca8b0385f1))
* **tempctrl:** expose PI gains through TempCtrlClient ([5c2f595](https://github.com/EIGSEP/eigsep_observing/commit/5c2f5955089826d2d9433764af313f8171c6dc1c))
* **tempctrl:** surface peltier health (stall_tripped + preflight) ([fcd88d8](https://github.com/EIGSEP/eigsep_observing/commit/fcd88d8d34144e2da47b3f2e17bf3d2668a5aa0a))
* **vna:** add save_vna_manual_h5 for bring-up artifacts ([416536c](https://github.com/EIGSEP/eigsep_observing/commit/416536c2c73997e79a224bae4d12eb3c9b3ae484))


### Bug Fixes

* **io:** add PI controller fields to tempctrl schema ([3f67e20](https://github.com/EIGSEP/eigsep_observing/commit/3f67e20d02e6c506677306717b365b64ea06edf6))
* **logging:** make configure_eig_logger(console=False) actually silence stderr ([6e36218](https://github.com/EIGSEP/eigsep_observing/commit/6e3621881219901b924f9aa226e83041128ab55c))
* **motor_manual:** recover integer step ladder from 0.1 floor ([e336315](https://github.com/EIGSEP/eigsep_observing/commit/e336315bd09377b37f8380f145b4a75d548be92b))
* **motor_scripts:** require_pico before issuing motor commands ([7cbc3fa](https://github.com/EIGSEP/eigsep_observing/commit/7cbc3faf1e137571c71461264a86816fc96fb1e0))
* **pico_preflight:** show per-channel tempctrl streams ([e2b87b0](https://github.com/EIGSEP/eigsep_observing/commit/e2b87b0bb142fc445ddbe515558202f979df9961))
* reconfigure existing logger handlers in configure_eig_logger ([8b71472](https://github.com/EIGSEP/eigsep_observing/commit/8b7147262e226a18a595cad5ece47eb823fc4e74))
* remove reference to old kwarg default_cfg ([93e1b1f](https://github.com/EIGSEP/eigsep_observing/commit/93e1b1f11c8e6a8cfb35b310186cf0bcde94e151))
* **scripts:** defer DummyPandaClient import in observation scripts ([6486438](https://github.com/EIGSEP/eigsep_observing/commit/64864384e51c16e9f6b6876f31af1ab0f0516e92))
* **scripts:** drop eager DummyPandaClient import in motor scripts ([2c66971](https://github.com/EIGSEP/eigsep_observing/commit/2c66971fa4cb3684d26cb0a92a5ff7a2b90e1d55))
* **scripts:** log producer-contract violations at ERROR ([c87a7ba](https://github.com/EIGSEP/eigsep_observing/commit/c87a7baf0d22f568b3fdb8445d409fe41115999a))
* **scripts:** silence console logs in repainting bring-up UIs ([041d081](https://github.com/EIGSEP/eigsep_observing/commit/041d0816376ca7f7798f3cccf47f530f49ab9d2a))
* **tempctrl_manual:** seed setpoints from firmware T_target ([f49b3e2](https://github.com/EIGSEP/eigsep_observing/commit/f49b3e2e8bfde918f25f50f1de2732f4ddf242ce))
* **vna_manual:** call build_vna_client helper instead of missing _build_client ([342f1b0](https://github.com/EIGSEP/eigsep_observing/commit/342f1b02baec7f91f4a7bc52beb433a16e1aff51))


### Documentation

* **tempctrl_manual:** surface picohost 3.4.0 enabled semantics ([23e6f05](https://github.com/EIGSEP/eigsep_observing/commit/23e6f05ff4db29e4a20a51d6895644912fe47591))

## [2.4.0](https://github.com/EIGSEP/eigsep_observing/compare/v2.3.0...v2.4.0) (2026-05-17)


### Features

* **observe:** decouple writer from panda ConfigStore ([6b795a2](https://github.com/EIGSEP/eigsep_observing/commit/6b795a2bcf4dbfdfcbe99739db524b019474a184))
* **scripts:** manual bring-up tools for rfswitch, lidar, imu, potmon, tempctrl ([#116](https://github.com/EIGSEP/eigsep_observing/issues/116)) ([8da0dc5](https://github.com/EIGSEP/eigsep_observing/commit/8da0dc56cc7fe78c4b1ad344b70605fd4eeabe9e))
* **tempctrl:** consume per-channel Redis streams; drop _avg_temp_metadata ([988cfc3](https://github.com/EIGSEP/eigsep_observing/commit/988cfc3a9ffc1d609b67a1b7d3fd80902d9eea7f))


### Bug Fixes

* **live-status:** source switch_schedule from Redis; suppress fake "next change" ([ceec4f3](https://github.com/EIGSEP/eigsep_observing/commit/ceec4f36676658034f6c330dc9b3d53a5b561197))

## [2.3.0](https://github.com/EIGSEP/eigsep_observing/compare/v2.2.0...v2.3.0) (2026-05-15)


### Features

* **scripts:** add pico_preflight for pre-observation pico status ([e022f4c](https://github.com/EIGSEP/eigsep_observing/commit/e022f4cdf374872b0cb86e537dd7eecefec1e76e))


### Bug Fixes

* ship panda_observe as eigsep-panda console script ([01dc314](https://github.com/EIGSEP/eigsep_observing/commit/01dc314f3024c2d333556bb14fa3a6f8bbd3cbab))
* stamp auto-generated h5 filenames in UTC with Z suffix ([c4ef1ee](https://github.com/EIGSEP/eigsep_observing/commit/c4ef1ee5d8bdc59a3be4c99be1fbebc23413b30a))
* **systemd:** gate observe + writer on chrony-wait.service ([bc700a4](https://github.com/EIGSEP/eigsep_observing/commit/bc700a49da04194917b3df43711c57eb302c0ce2))

## [2.2.0](https://github.com/EIGSEP/eigsep_observing/compare/v2.1.0...v2.2.0) (2026-05-13)


### Features

* **live-status:** mirror ground-side ERRORs to dashboard event log ([b409439](https://github.com/EIGSEP/eigsep_observing/commit/b409439cd0d909b73677083bbce567717703f520))


### Bug Fixes

* wrap status_log_handler.close and sentinel enqueue in try/except/finally ([8227ed5](https://github.com/EIGSEP/eigsep_observing/commit/8227ed5838c198c658e49d7b39da55a68c4935e6))

## [2.1.0](https://github.com/EIGSEP/eigsep_observing/compare/v2.0.1...v2.1.0) (2026-05-12)


### Features

* **live-status:** add first-order Y-factor calibration toggle ([2c6712a](https://github.com/EIGSEP/eigsep_observing/commit/2c6712a6935501adc7410628f57155f0a28c9d98))
* **live-status:** add VNA S11 pane with ideal-OSL calibration ([#101](https://github.com/EIGSEP/eigsep_observing/issues/101)) ([dae4592](https://github.com/EIGSEP/eigsep_observing/commit/dae45923a0ff5037ed5371fae71793d26fa6a99c))
* **live-status:** take noise diode ENR in dB at YAML boundary ([0a7d5fb](https://github.com/EIGSEP/eigsep_observing/commit/0a7d5fb30f6dad783238e374c52548c8313c987e))


### Bug Fixes

* **fpga:** propagate producer-thread failures so systemd restarts ([#102](https://github.com/EIGSEP/eigsep_observing/issues/102)) ([f0e744a](https://github.com/EIGSEP/eigsep_observing/commit/f0e744a305881b080d4f376e86da68b94fda171e))
* **live-status:** log ERROR on cal contract violations ([c140ff7](https://github.com/EIGSEP/eigsep_observing/commit/c140ff7712519eb956fa588302244735f514c632))

## [2.0.1](https://github.com/EIGSEP/eigsep_observing/compare/v2.0.0...v2.0.1) (2026-05-03)


### Bug Fixes

* migrate SnapAdc construction to casperfpga 0.7.1 device_info API ([#92](https://github.com/EIGSEP/eigsep_observing/issues/92)) ([404ce17](https://github.com/EIGSEP/eigsep_observing/commit/404ce17ef4dcc7527b09b387fde326ad47614552))

## [2.0.0](https://github.com/EIGSEP/eigsep_observing/compare/v1.0.0...v2.0.0) (2026-05-01)


### ⚠ BREAKING CHANGES

* **capture-spectrum:** write HDF5 with full header and optional panda metadata ([#83](https://github.com/EIGSEP/eigsep_observing/issues/83))
* split EigsepRedis into Transport + writer/reader classes
* callers using corr/VNA methods must switch from EigsepRedis to EigsepObsRedis. eigsep_observing.EigsepRedis now resolves to the base bus class (no corr/VNA methods).

### Features

* **capture-spectrum:** write HDF5 with full header and optional panda metadata ([#83](https://github.com/EIGSEP/eigsep_observing/issues/83)) ([5b6e5da](https://github.com/EIGSEP/eigsep_observing/commit/5b6e5da6791c4f9abd3b06c3326da42bcf39d051))
* **corr:** gate CorrWriter.add on sync_time ([#47](https://github.com/EIGSEP/eigsep_observing/issues/47)) ([a189f0e](https://github.com/EIGSEP/eigsep_observing/commit/a189f0edc31599cecd4ddbac537997a8c3353ca7))
* **corr:** stamp header_upload_unix, publish on every state change ([#51](https://github.com/EIGSEP/eigsep_observing/issues/51)) ([757b541](https://github.com/EIGSEP/eigsep_observing/commit/757b5416c99ca794ff0af27d1b0089f503735742))
* **dev:** local fake-observation harness sharing one Redis ([#79](https://github.com/EIGSEP/eigsep_observing/issues/79)) ([11b2307](https://github.com/EIGSEP/eigsep_observing/commit/11b23073af310d22b6ef08e9cb6413e36867d1ef))
* **fpga-init:** close attach-path validation gap via Redis-as-truth ([e95d89a](https://github.com/EIGSEP/eigsep_observing/commit/e95d89a7bc063263aaea806569c0de4d3280915b))
* **fpga-init:** collapse init flags into --reinit, rehydrate sync_time on attach ([17a0bef](https://github.com/EIGSEP/eigsep_observing/commit/17a0befad2c4fcbf7e64b0fa05b191314230867e))
* **fpga:** publish ADC diagnostics to Redis ([b8fe8ec](https://github.com/EIGSEP/eigsep_observing/commit/b8fe8ec773c9e7c348d9cec27a3495f853fd8b68))
* **live-status:** add local Flask dashboard for field deployments ([83eb3e5](https://github.com/EIGSEP/eigsep_observing/commit/83eb3e5a13b8577237bcb44cd6f4ac30c65dc6c6))
* **live-status:** surface active panda run_tag in dashboard header ([5ac96e0](https://github.com/EIGSEP/eigsep_observing/commit/5ac96e0f7b99a110bc45540404c90ee453b2c978))
* **motor:** migrate scan + manual scripts from picohost via PicoProxy ([#71](https://github.com/EIGSEP/eigsep_observing/issues/71)) ([287919f](https://github.com/EIGSEP/eigsep_observing/commit/287919fdeb9ce33234d5079ba935e8b1ef43c6d5))
* **observer:** split corr header-fetch failure modes with watchdog ([#48](https://github.com/EIGSEP/eigsep_observing/issues/48)) ([b17ae4d](https://github.com/EIGSEP/eigsep_observing/commit/b17ae4d9882b2ba527ba859fb0e6688bbd94f6c6))
* **observer:** unify SNAP liveness under a single deadline in record_corr_data ([8924a99](https://github.com/EIGSEP/eigsep_observing/commit/8924a999d9b8656ba0334b02dae548f15da4b528))
* **observing:** record run_tag + obs_config on every saved file ([f1dda9f](https://github.com/EIGSEP/eigsep_observing/commit/f1dda9f118ef401e0e835a6882656feeb518c1c6))
* **panda-client:** add switch_session context manager ([#59](https://github.com/EIGSEP/eigsep_observing/issues/59)) ([bddbb32](https://github.com/EIGSEP/eigsep_observing/commit/bddbb32553addbe974a801d46c9e34231cc218fc))
* **panda-client:** bump status stream maxlen 5 → 100 ([51aafcb](https://github.com/EIGSEP/eigsep_observing/commit/51aafcb10114256c9bc1d72c53ef97776a283e62))
* **panda-client:** route panda warnings to ground via status stream; fix logger setup ([ccae27b](https://github.com/EIGSEP/eigsep_observing/commit/ccae27bad8fea604e96b707a2a5661a434f3a06a))
* **panda-client:** self-validate VNA S11 payload; emit violations to ground via status stream ([75e129a](https://github.com/EIGSEP/eigsep_observing/commit/75e129ad89532e55efc5086cc39506afe14c11f7))
* **panda:** add motor_loop + use_motor gating to PandaClient ([#72](https://github.com/EIGSEP/eigsep_observing/issues/72)) ([b03c54c](https://github.com/EIGSEP/eigsep_observing/commit/b03c54c16a4b5935c19a97dff0594aa4afb66871))
* **panda:** add tempctrl_loop + use_tempctrl gating to PandaClient ([#74](https://github.com/EIGSEP/eigsep_observing/issues/74)) ([58cad56](https://github.com/EIGSEP/eigsep_observing/commit/58cad5685d0d7634fae16125165b969b1ed25249))
* **panda:** enforce boot-time RFANT + vna_loop exception recovery ([#70](https://github.com/EIGSEP/eigsep_observing/issues/70)) ([154cf8b](https://github.com/EIGSEP/eigsep_observing/commit/154cf8ba94614c851974e800c6e3b6218fc5f679))
* **panda:** motion-switch coordinator + VNA-position-sweep / no-switch-observation scripts ([0b49a34](https://github.com/EIGSEP/eigsep_observing/commit/0b49a34e773692f52a5210960c14e25c00630046))
* **recovery:** systemd supervisor + non-suicidal liveness watchdog ([815a5cf](https://github.com/EIGSEP/eigsep_observing/commit/815a5cfd56a16b9a22c2b32888446b345f37756a))
* **redis:** warn on corr stream gaps via acc_cnt monotonicity ([2b8279d](https://github.com/EIGSEP/eigsep_observing/commit/2b8279d79e4fb7792e129ec199a50e72a9ddb496))
* **redis:** warn on stale metadata snapshot reads ([#62](https://github.com/EIGSEP/eigsep_observing/issues/62)) ([91b855c](https://github.com/EIGSEP/eigsep_observing/commit/91b855cea228bb8e9ebe3f3666b2cd541e73a03a))
* **redis:** warn on stale metadata stream drains ([87da4d1](https://github.com/EIGSEP/eigsep_observing/commit/87da4d1067c8fa5f8abe24e11338f0d3b9aff22e))


### Bug Fixes

* change config ownership to Panda ([38bee1e](https://github.com/EIGSEP/eigsep_observing/commit/38bee1e44a106f65a5e7f82b9294c23ef9a518ae))
* **fpga:** align adc_stats sensor_name with schema key ([d95dd5a](https://github.com/EIGSEP/eigsep_observing/commit/d95dd5a37ecc46264eaa1b847f1ffc86d03beabb))
* **fpga:** disable ADC diag publishers on first failure ([f177dff](https://github.com/EIGSEP/eigsep_observing/commit/f177dfff66fdaa61157d77f57542207c64f7356f))
* **fpga:** serialize TAPCP transactions via lock proxy ([896b0f0](https://github.com/EIGSEP/eigsep_observing/commit/896b0f00b246576b311601afb6177373a3791ac2))
* **io:** add motor sensor schema, validate producer contract ([d3e7b26](https://github.com/EIGSEP/eigsep_observing/commit/d3e7b2611e0c26a25ad18a044018e63e9e294b24))
* **io:** strict float check in _validate_metadata ([ff5c223](https://github.com/EIGSEP/eigsep_observing/commit/ff5c2231ed9c55c591e87fdd4f3809f18dc45781))
* **live-status:** tolerate bus-down at startup via lazy Transport ([596e04e](https://github.com/EIGSEP/eigsep_observing/commit/596e04eef625989359cdbb9dcd69d10184e3197e))
* **live-status:** unstall the plotter, pin axes, label by wiring ([a2433cd](https://github.com/EIGSEP/eigsep_observing/commit/a2433cdd7e264c74b01db2a01753544a66fb8608))
* **panda-client:** drop dead VNA poll, stop mutating cfg, add stop() ([1e6fd56](https://github.com/EIGSEP/eigsep_observing/commit/1e6fd5646a002eee91a2973aa835af32334f9cf4))
* **panda-client:** warn on failed VNA switch-back ([b7f98a4](https://github.com/EIGSEP/eigsep_observing/commit/b7f98a4588ce94ab8bb761aae7cece66de8ad331))
* **panda:** survive proxy errors in RF switch observing loops ([0530d79](https://github.com/EIGSEP/eigsep_observing/commit/0530d7962deb93aa5197520308813ead57820780))
* restrict add_metadata to json serializable objects only ([52d488b](https://github.com/EIGSEP/eigsep_observing/commit/52d488bd3a36e6b2c4aa8d861d71575279bb255b))
* **scripts:** clean up panda client when validation guards fail ([cacd0f5](https://github.com/EIGSEP/eigsep_observing/commit/cacd0f5225568524a9d9be998e6edffc0c0fb41c))
* seperate metadata streams from data streams in EigRedis class ([b8cc1ed](https://github.com/EIGSEP/eigsep_observing/commit/b8cc1ed575a500f057fa1cc653beee1a3d45d1d3))
* skip testing import if missing test dependencies ([1cee97a](https://github.com/EIGSEP/eigsep_observing/commit/1cee97a6f317e5747ed08e27d73c8a90f8d1e0ba))
* **testing:** wrap dummy IMU partial in staticmethod for Python 3.14 ([2b50238](https://github.com/EIGSEP/eigsep_observing/commit/2b5023802eccef5f03f8285e83ca81274e2ae9ab))
* wrap testing imports in try/except and fail fast if not available when running in dummy mode ([0d7d259](https://github.com/EIGSEP/eigsep_observing/commit/0d7d259529cdca2886233c0d9e0da245ec644563))


### Documentation

* make inline comment about corr watchdog less verbose ([67a037d](https://github.com/EIGSEP/eigsep_observing/commit/67a037d6f1dff3b64656ca594be0592385e0c0c9))
* **observe:** clarify panda start-order is not required ([22fa533](https://github.com/EIGSEP/eigsep_observing/commit/22fa533eeee93ab2b808e4f956bddec94dc68ffd))


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
