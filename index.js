const { start, dispatch, stop, spawnStateless, spawn } = require('nact');
const fs = require('fs');
const { exit } = require('process');
const sleep = (time) => new Promise((res) => setTimeout(res, time));

//#region Setup

const file_num = 2;
const data = JSON.parse(fs.readFileSync(`test_files/IFF93_SimonaviciusRokas_L1_dat_${file_num}.json`).toString('utf8'));
const N = data.length;
const worker_count = (() => {
  if (N <= 2) {
    return 2;
  } else {
    return Math.floor(N / 4);
  }
})();

const system = start();

//#endregion Setup

//#region Actions

const DistributorActions = {
  'begin': async (state, msg, ctx) => {
    return {
      workerCount: msg.worker_count,
      workersDone: 0,
      workerIdx: 0,
      finishedReceiving: false,
      collector: spawnCollector(ctx.self),
      printer: spawnPrinter(ctx.self),
      workers: [...Array(msg.worker_count).keys()].map((val) => {
        return spawnWorker(ctx.self, val)
      })
    }
  },

  'distributor_receive_main': async (state, msg, ctx) => {
    // Receive data from main and send to worker
    dispatch(
      state.workers[state.workerIdx], {
        action: 'worker_receive_distributor', 
        data: msg.data,
        idx: state.workerIdx,
      }
    );

    return {
      ...state,
      workerIdx: (state.workerIdx + 1) % state.workerCount,
    };
  },

  'distributor_receive_worker': async (state, msg, ctx) => {
    if (msg.done) {
      if (state.workersDone + 1 === state.workerCount) {
        // all workers done so request array from collector
        dispatch(state.collector, {
          action: 'collector_receive_end',
        });
      }

      return {
        ...state,
        workersDone: state.workersDone + 1,
      }
    } else {
      if (msg.res) {
        // result passes filter, send to collector
        dispatch(state.collector, {
          action: 'collector_receive_distributor',
          data: msg.res,
          orig: msg.orig,
        });
      }

      // else result was filtered out
      return {
        ...state,
      }
    }
  },

  'distributor_receive_end': async (state, msg, ctx) => {
    // queue up all workers to end
    state.workers.forEach((val) => {
      dispatch(val, { action: 'worker_receive_end' });
    });

    return {
      ...state,
      finishedReceiving: true,
    };
  },

  'distributor_receive_array': async (state, msg, ctx) => {
    // distributor receives final array from collector and sends it to printer
    dispatch(state.printer, {
      action: 'printer_receive_end',
      data: msg.data,
    });

    return {
      ...state,
    };
  },
};

const WorkerActions = {
  'worker_receive_distributor': async (msg, ctx) => {
    // Original c++ function generated random numbers and did bit operation
    // Simplified to just base64 encoding for brevity
    const hardFunction = async (data) => {
      const string = data.password + (data.passes + data.salt);
      const buffer = Buffer.from(string);
      const base64 = buffer.toString('base64');

      //await sleep(1000); // for testing

      return base64.replace(/=/g, '');
    };

    const zeroInt = '0'.charCodeAt(0);
    const nineInt = '9'.charCodeAt(0);

    const result = await hardFunction(msg.data);
    const firstChar = result.charCodeAt(0);
    const res = (firstChar >= zeroInt && firstChar <= nineInt) ? null : result;

    // if data doesn't fit filter then a null is returned to distributor
    dispatch(
      ctx.parent, {
        action: 'distributor_receive_worker', idx: msg.idx, res, orig: msg.data
      }
    );
  },

  'worker_receive_end': async (msg, ctx) => {
    // used as synchronization for workers, because all worker messages are queued up
    dispatch(ctx.parent, {
      action: 'distributor_receive_worker',
      done: true
    });
  }
};

const CollectorActions = {
  'collector_receive_distributor': async (state, msg, ctx) => {
    // appends new item to inner array
    const array = state?.array ?? []; // if state does not exist, then it is initialized as []

    return {
      ...state,
      array: array.concat([{ data: msg.data, orig: msg.orig }]),
    };
  },
  'collector_receive_end': async (state, msg, ctx) => {
    // sends final array to distributor
    dispatch(ctx.parent, {
      action: 'distributor_receive_array',
      data: state?.array ?? [],
    });

    return {
      ...state,
    };
  },
};

const PrinterActions = {
  'printer_receive_end': async (msg, ctx) => {
    // receives final array
    const data = msg.data;
    const stream = fs.createWriteStream(`test_files/IFF93_SimonaviciusRokas_L1_rez_${file_num}.txt`);
    
    if (!stream) {
      console.error("Couldn't open file to write to. Exiting...");
      exit(1);
    }

    const header = "Password".padStart(30, ' ') + "Passes".padStart(20, ' ') + "Salt".padStart(20, ' ') + "Hash".padStart(50, ' ') + '\n' + '-'.repeat(120) + '\n';

    stream.write(header, (err) => {
      if (err) {
        console.error(err);
        exit(2);
      }
    });

    data.forEach(async (val) => {
      const line = val.orig.password.padStart(30, ' ') + val.orig.passes.toString().padStart(20, ' ') + val.orig.salt.toString().padStart(20, ' ') + val.data.padStart(50, ' ') + '\n';
      stream.write(line, (err) => {
        if (err) {
          console.error(err);
          exit(2);
        }
      });
    });
  },
};

//#endregion Actions

//#region Actor_spawners

const spawnDistributor = (system) => {
  return spawn(
    system,
    async (state, msg, ctx) => {
      const actionType = msg.action;
      const action = DistributorActions[actionType];

      return await action(state, msg, ctx);
    },
    'distributor'
  );
}

const spawnWorker = (system, num) => {
  return spawnStateless(
    system,
    async (msg, ctx) => {
      const actionType = msg.action;
      const action = WorkerActions[actionType];

      await action(msg, ctx);
    },
    `worker_${num + 1}`
  );
}

const spawnCollector = (system) => {
  return spawn(
    system,
    async (state, msg, ctx) => {
      const actionType = msg.action;
      const action = CollectorActions[actionType];
      
      return await action(state, msg, ctx);
    },
    'collector'
  )
}

const spawnPrinter = (system) => {
  return spawnStateless(
    system,
    async (msg, ctx) => {
      const actionType = msg.action;
      const action = PrinterActions[actionType];
      
      await action(msg, ctx);
    },
    'printer'
  )
}

//#endregion Actor_spawners

// ----------------------------------------------------------------------------------
// --- Actor execution starts here --------------------------------------------------
// ----------------------------------------------------------------------------------

const distributor = spawnDistributor(system);
dispatch(distributor, { action: 'begin', worker_count, item_count: N });

data.forEach(async (val) => {
  dispatch(distributor, { action: 'distributor_receive_main', data: val });
});

dispatch(distributor, { action: 'distributor_receive_end' });