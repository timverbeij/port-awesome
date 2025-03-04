import ReactEngine from './visualisation/react/engine';
import ReactFactory from './visualisation/react/factory';
import WorkerProcessingEngine from './processing/worker_engine';
import CommandRouter from './command_router';
export default class Assembly {
    constructor(worker, system) {
        const sessionId = String(Date.now());
        this.visualisationEngine = new ReactEngine(new ReactFactory());
        this.router = new CommandRouter(system, this.visualisationEngine);
        this.processingEngine = new WorkerProcessingEngine(sessionId, worker, this.router);
    }
}
