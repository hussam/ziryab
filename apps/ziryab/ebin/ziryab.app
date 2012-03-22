{application, ziryab,
   [
      {description, "Scalabe strongly consistent KVS"},
      {vsn, "0.1"},
      {modules, [
         ziryab,
         ziryab_app,
         ziryab_sup,
         ziryab_console,
         ziryab_cluster_manager,
         ziryab_core,
         ziryab_tracker,
         ziryab_config,
         core_cmd,
         core_fsm,
         core_utils
         ]},
      {registered, [ziryab_tracker, ziryab_cluster_manager]},
      {applications, [kernel, stdlib]},
      {mod, { ziryab_app, []}}
   ]
}.
