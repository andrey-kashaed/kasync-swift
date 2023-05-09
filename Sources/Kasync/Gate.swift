//
// DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
//
// Copyright (c) 2023 Andrey Kashaed. All rights reserved.
//
// The contents of this file are subject to the terms of the
// Common Development and Distribution License 1.0 (the "License").
// You may not use this file except in compliance with the License.
//
// You can obtain a copy of the License at
// https://opensource.org/licenses/CDDL-1.0 or LICENSE.txt.
//

import Foundation

public protocol Source {
    
    associatedtype Input
    associatedtype Output
    
    var isSealed: Bool { get }
    
    @discardableResult
    func process(consumerId: UInt64, spec: TSpec<Input>, operation: (Input) async throws -> Output) async throws -> Output
    
    @discardableResult
    func receive(consumerId: UInt64, spec: TSpec<Input>, instantOutput: Output) async throws -> Input
    
    func processor(consumerId: UInt64, spec: TSpec<Input>, operation: @escaping (Input) async throws -> Output) -> AnyAsyncSequence<Output>
    
    func receiver(consumerId: UInt64, spec: TSpec<Input>, instantOutput: Output) -> AnyAsyncSequence<Input>
    
}

public extension Source {
    
    @discardableResult
    func process(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), spec: TSpec<Input> = TrueSpec(), operation: (Input) async throws -> Output) async throws -> Output {
        try await process(consumerId: consumerId, spec: spec, operation: operation)
    }
    
    @discardableResult
    func process<SubInput>(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), operation: (SubInput) async throws -> Output) async throws -> Output {
        try await process(consumerId: consumerId, spec: TSpec(isSatisfiedBy: { $0 is SubInput }), operation: { try await operation($0 as! SubInput) })
    }
    
    @discardableResult
    func receive(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), spec: TSpec<Input> = TrueSpec(), instantOutput: Output) async throws -> Input {
        try await receive(consumerId: consumerId, spec: spec, instantOutput: instantOutput)
    }
    
    @discardableResult
    func receive<SubInput>(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), instantOutput: Output) async throws -> SubInput {
        try await receive(consumerId: consumerId, spec: TSpec(isSatisfiedBy: { $0 is SubInput }), instantOutput: instantOutput) as! SubInput
    }
    
    func processor(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), spec: TSpec<Input> = TrueSpec(), operation: @escaping (Input) async throws -> Output) -> AnyAsyncSequence<Output> {
        processor(consumerId: consumerId, spec: spec, operation: operation)
    }
    
    func receiver(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), spec: TSpec<Input> = TrueSpec(), instantOutput: Output) -> AnyAsyncSequence<Input> {
        receiver(consumerId: consumerId, spec: spec, instantOutput: instantOutput)
    }
    
    func processor<SubInput>(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), operation: @escaping (SubInput) async throws -> Output) -> AnyAsyncSequence<Output> {
        processor(consumerId: consumerId, spec: TSpec(isSatisfiedBy: { $0 is SubInput }), operation: { try await operation($0 as! SubInput) })
    }
    
    func receiver<SubInput>(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), instantOutput: Output) -> AnyAsyncSequence<SubInput> {
        receiver(consumerId: consumerId, spec: TSpec(isSatisfiedBy: { $0 is SubInput }), instantOutput: instantOutput).map { $0 as! SubInput }.eraseToAnyAsyncSequence()
    }
    
}

public extension Source where Output == Void {
    
    func receive(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), spec: TSpec<Input> = TrueSpec()) async throws -> Input {
        try await receive(consumerId: consumerId, spec: spec, instantOutput: ())
    }
    
    func receive<SubInput>(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max)) async throws -> SubInput {
        try await receive(consumerId: consumerId, instantOutput: ())
    }
    
    func receiver(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), spec: TSpec<Input> = TrueSpec()) -> AnyAsyncSequence<Input> {
        receiver(consumerId: consumerId, spec: spec, instantOutput: ())
    }
    
    func receiver<SubInput>(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max)) -> AnyAsyncSequence<SubInput> {
        receiver(consumerId: consumerId, instantOutput: ())
    }
    
}

public final class AnySource<Input, Output>: Source {
    
    private let gate: Gate<Input, Output>
    
    fileprivate init(gate: Gate<Input, Output>) {
        self.gate = gate
    }
    
    public var isSealed: Bool {
        gate.isSealed
    }
    
    public func process(consumerId: UInt64, spec: TSpec<Input>, operation: (Input) async throws -> Output) async throws -> Output {
        try await gate.process(consumerId: consumerId, spec: spec, operation: operation)
    }
    
    public func receive(consumerId: UInt64, spec: TSpec<Input>, instantOutput: Output) async throws -> Input {
        try await gate.receive(consumerId: consumerId, spec: spec, instantOutput: instantOutput)
    }
    
    public func processor(consumerId: UInt64, spec: TSpec<Input>, operation: @escaping (Input) async throws -> Output) -> AnyAsyncSequence<Output> {
        gate.processor(consumerId: consumerId, spec: spec, operation: operation)
    }
    
    public func receiver(consumerId: UInt64, spec: TSpec<Input>, instantOutput: Output) -> AnyAsyncSequence<Input> {
        gate.receiver(consumerId: consumerId, spec: spec, instantOutput: instantOutput)
    }
    
}

public protocol Drain {
    
    associatedtype Input
    associatedtype Output
    
    var isSealed: Bool { get }
    
    func send(producerId: UInt64, _ provider: @escaping () async throws -> Input) async throws -> Output
    
    func sender(producerId: UInt64, provider: AnyAsyncSequence<Input>) -> AnyAsyncSequence<Output>
    
}

public extension Drain {
    
    func send(producerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), _ provider: @autoclosure @escaping () async throws -> Input) async throws -> Output {
        try await send(producerId: producerId, provider)
    }
    
    func send(producerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), _ provider: @escaping () async throws -> Input) async throws -> Output {
        try await send(producerId: producerId, provider)
    }
    
    func trySend(producerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), _ provider: @autoclosure @escaping () async throws -> Input) async -> Output? {
        do {
            return try await send(producerId: producerId, provider)
        } catch {
            return nil
        }
    }
    
    func trySend(producerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), _ provider: @escaping () async throws -> Input) async -> Output? {
        do {
            return try await send(producerId: producerId, provider)
        } catch {
            return nil
        }
    }

    func sender(producerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), provider: AnyAsyncSequence<Input>) -> AnyAsyncSequence<Output> {
        sender(producerId: producerId, provider: provider)
    }
    
}

public final class AnyDrain<Input, Output>: Drain {
    
    private let gate: Gate<Input, Output>
    
    fileprivate init(gate: Gate<Input, Output>) {
        self.gate = gate
    }
    
    public var isSealed: Bool {
        gate.isSealed
    }
    
    public func send(producerId: UInt64, _ provider: @escaping () async throws -> Input) async throws -> Output {
        try await gate.send(producerId: producerId, provider)
    }
    
    public func sender(producerId: UInt64, provider: AnyAsyncSequence<Input>) -> AnyAsyncSequence<Output> {
        gate.sender(producerId: producerId, provider: provider)
    }
    
}

public enum GateError: Error {
    case sealedGate
    case discardedInput
    case discardedOutput
}

extension GateError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .sealedGate:
            return "Gate is sealed"
        case .discardedInput:
            return "Input is discarded"
        case .discardedOutput:
            return "Output is discarded"
        }
    }
}

public class Gate<Input, Output>: Source, Drain, CustomDebugStringConvertible {
    
    public enum Mode: Equatable {
        case cumulative(capacity: Int = Int.max)
        case retainable(capacity: Int = Int.max)
        case transient(capacity: Int = Int.max)
        var capacity: Int {
            switch self {
            case .cumulative(let capacity):
                return capacity
            case .retainable(let capacity):
                return capacity
            case .transient(let capacity):
                return capacity
            }
        }
    }
    
    public enum Scheme {
        case unicast
        case broadcast(combiner: (([Output]) -> Output)? = nil)
        case multicast(combiner: (([Output]) -> Output)? = nil)
        case anycast
    }
    
    fileprivate struct Demand {
        let consumerId: UInt64
        let inputSpec: TSpec<Input>
    }
    
    fileprivate struct Supply {
        let producerId: UInt64
        let input: Input
    }
    
    fileprivate struct Reply {
        let consumerId: UInt64
        let result: Result<Output, Error>
    }
    
    fileprivate struct Transmission: Equatable {
        let producerId: UInt64
        let consumerId: UInt64
    }
    
    private let mode: Mode
    private let scheme: Scheme
    private var produceContinuations: [UInt64: CheckedContinuation<Output, Error>] = [:]
    private var consumeContinuations: [UInt64: CheckedContinuation<Input, Error>] = [:]
    private var attachedConsumerIds: [UInt64: TSpec<Input>] = [:]
    private var discardedConsumerIds: [UInt64] = []
    private var demandQueue: [Demand] = []
    private var supplyQueue: [Supply] = []
    private var replyCollection: [Reply] = []
    private var transmissions: [Transmission] = []
    private var sealError: Error? = nil
    private let lock = NSRecursiveLock()
    
    public init(mode: Mode, scheme: Scheme) {
        self.mode = mode
        self.scheme = scheme
    }
    
    public func seal() {
        seal(GateError.sealedGate)
    }
    
    public var debugDescription: String {
        lock.synchronized {
            "\(type(of: self)) demandQueue.count: \(demandQueue.count), supplyQueue.count: \(supplyQueue.count), produceContinuations.count: \(produceContinuations.count), consumeContinuations.count: \(consumeContinuations.count), replyCollection.count: \(replyCollection.count), transmissions.count: \(transmissions.count)"
        }
    }
    
    public func seal(_ error: Error) {
        lock.synchronized {
            sealError = error
            demandQueue.removeAll()
            supplyQueue.removeAll()
            replyCollection.removeAll()
            consumeContinuations.values.forEach { receiveContinuation in
                receiveContinuation.resume(throwing: error)
            }
            consumeContinuations.removeAll()
            produceContinuations.values.forEach { sendContinuation in
                sendContinuation.resume(throwing: error)
            }
            produceContinuations.removeAll()
            transmissions.removeAll()
        }
    }
    
    public var isSealed: Bool {
        lock.synchronized { sealError != nil }
    }
    
    public func discardProducer(producerId: UInt64) {
        lock.synchronized {
            let _ = dequeueSupply(producerId: producerId)
            guard let produceContinuation = removeProduceContinuation(producerId: producerId) else {
                return
            }
            produceContinuation.resume(throwing: GateError.discardedInput)
        }
    }
    
    public func discardConsumer(consumerId: UInt64) {
        lock.synchronized {
            let _ = dequeueDemand(consumerId: consumerId)
            if let consumeContinuation = removeConsumeContinuation(consumerId: consumerId) {
                consumeContinuation.resume(throwing: GateError.discardedOutput)
                return
            }
            if hasConsumer(consumerId: consumerId) {
                addDiscardedConsumerId(consumerId)
            }
        }
    }
    
    public func sender(producerId: UInt64, provider: AnyAsyncSequence<Input>) -> AnyAsyncSequence<Output> {
        let providerIterator = provider.makeAsyncIterator()
        return AnyAsyncSequence(
            next: { [weak self] in
                guard let input = try await providerIterator.next() else { return nil }
                return try await self?.send(producerId: producerId, { input })
            }
        )
    }
    
    public func send(producerId: UInt64, _ provider: @escaping () async throws -> Input) async throws -> Output {
        let input = try await provider()
        return try await produce(input, producerId: producerId)
    }
    
    public func processor(consumerId: UInt64, spec: TSpec<Input>, operation: @escaping (Input) async throws -> Output) -> AnyAsyncSequence<Output> {
        attachConsumerId(consumerId, inputSpec: spec)
        return AnyAsyncSequence(
            next: { [weak self] in
                try await self?.process(spec: spec, operation: operation, consumerId: consumerId)
            },
            onClose: { [weak self] in
                self?.detachConsumerId(consumerId)
            }
        )
    }
    
    public func receiver(consumerId: UInt64, spec: TSpec<Input>, instantOutput: Output) -> AnyAsyncSequence<Input> {
        self.attachConsumerId(consumerId, inputSpec: spec)
        return AnyAsyncSequence(
            next: { [weak self] in
                try await self?.receive(spec: spec, instantOutput: instantOutput, consumerId: consumerId)
            },
            onClose: { [weak self] in
                self?.detachConsumerId(consumerId)
            }
        )
    }
    
    @discardableResult
    public func process(consumerId: UInt64, spec: TSpec<Input>, operation: (Input) async throws -> Output) async throws -> Output {
        attachConsumerId(consumerId, inputSpec: spec)
        defer { detachConsumerId(consumerId) }
        return try await process(spec: spec, operation: operation, consumerId: consumerId)
    }
    
    @discardableResult
    public func receive(consumerId: UInt64, spec: TSpec<Input>, instantOutput: Output) async throws -> Input {
        attachConsumerId(consumerId, inputSpec: spec)
        defer { detachConsumerId(consumerId) }
        return try await receive(spec: spec, instantOutput: instantOutput, consumerId: consumerId)
    }
    
    @discardableResult
    private func process(spec: TSpec<Input>, operation: (Input) async throws -> Output, consumerId: UInt64) async throws -> Output {
        do {
            let input = try await consume(spec, consumerId: consumerId)
            let output = try await operation(input)
            respond(consumerId: consumerId, result: .success(output))
            return output
        } catch {
            respond(consumerId: consumerId, result: .failure(error))
            throw error
        }
    }
    
    @discardableResult
    private func receive(spec: TSpec<Input>, instantOutput: Output, consumerId: UInt64) async throws -> Input {
        do {
            let input = try await consume(spec, consumerId: consumerId)
            respond(consumerId: consumerId, result: .success(instantOutput))
            return input
        } catch {
            respond(consumerId: consumerId, result: .failure(error))
            throw error
        }
    }
    
    private func produce(_ input: Input, producerId: UInt64) async throws -> Output {
        try checkSeal()
        defer {
            removeProduceContinuation(producerId: producerId)
        }
        try Task.checkCancellation()
        let output = try await withCancellableCheckedThrowingContinuation() { [weak self] (continuation: CheckedContinuation<Output, Error>, cancellation: Cancellation) -> Void in
            cancellation.onCancel = { [weak self] in
                self?.removeProduceContinuation(producerId: producerId)?.resume(throwing: CancellationError())
            }
            guard let self else { return }
            self.lock.synchronized {
                self.addProduceContinuation(continuation, producerId: producerId)
                self.queueSupply(Supply(producerId: producerId, input: input))
                self.startTransmissionsIfRequired()
            }
        }
        try Task.checkCancellation()
        return output
    }
    
    private func consume(_ inputSpec: TSpec<Input>, consumerId: UInt64) async throws -> Input {
        try checkSeal()
        defer {
            removeConsumeContinuation(consumerId: consumerId)
        }
        try Task.checkCancellation()
        let input = try await withCancellableCheckedThrowingContinuation() { [weak self] (continuation: CheckedContinuation<Input, Error>, cancellation: Cancellation) -> Void in
            cancellation.onCancel = { [weak self] in
                self?.removeConsumeContinuation(consumerId: consumerId)?.resume(throwing: CancellationError())
            }
            guard let self else { return }
            self.lock.synchronized {
                if self.hasDiscardedConsumerId(consumerId) {
                    self.removeDiscardedConsumerId(consumerId)
                    continuation.resume(throwing: GateError.discardedOutput)
                    return
                }
                self.addConsumeContinuation(continuation, consumerId: consumerId)
                self.queueDemand(Demand(consumerId: consumerId, inputSpec: inputSpec))
                self.startTransmissionsIfRequired()
            }
        }
        try Task.checkCancellation()
        return input
    }
    
    private func respond(consumerId: UInt64, result: Result<Output, Error>) {
        lock.synchronized {
            if transmissions.contains(where: { $0.consumerId == consumerId }) {
                addReply(Reply(consumerId: consumerId, result: result))
            }
        }
    }
    
    private func beginTransmission(producerId: UInt64, consumerId: UInt64) {
        lock.synchronized {
            transmissions.append(Transmission(producerId: producerId, consumerId: consumerId))
        }
    }
    
    private func endTransactions(producerId: UInt64) -> [Transmission] {
        lock.synchronized {
            let index = transmissions.partition(by: { $0.producerId == producerId })
            if index == transmissions.count { return [] }
            let endingTransmissions = transmissions[index...]
            transmissions = Array(transmissions[..<index])
            return Array(endingTransmissions)
        }
    }
    
    private func hasConsumeContinuation() -> Bool {
        lock.synchronized {
            !consumeContinuations.isEmpty
        }
    }
    
    private func addConsumeContinuation(_ continuation: CheckedContinuation<Input, Error>, consumerId: UInt64) {
        lock.synchronized {
            consumeContinuations[consumerId] = continuation
        }
    }
    
    @discardableResult
    private func removeConsumeContinuation(consumerId: UInt64) -> CheckedContinuation<Input, Error>? {
        lock.synchronized {
            consumeContinuations.removeValue(forKey: consumerId)
        }
    }
    
    private func attachConsumerId(_ consumerId: UInt64, inputSpec: TSpec<Input>) {
        lock.synchronized {
            attachedConsumerIds[consumerId] = inputSpec
        }
    }
    
    private func detachConsumerId(_ consumerId: UInt64) {
        lock.synchronized {
            attachedConsumerIds.removeValue(forKey: consumerId)
            removeDiscardedConsumerId(consumerId)
        }
    }
    
    private func hasDiscardedConsumerId(_ consumerId: UInt64) -> Bool {
        lock.synchronized {
            discardedConsumerIds.contains(consumerId)
        }
    }
    
    private func addDiscardedConsumerId(_ consumerId: UInt64) {
        lock.synchronized {
            discardedConsumerIds.append(consumerId)
        }
    }
    
    private func removeDiscardedConsumerId(_ consumerId: UInt64) {
        lock.synchronized {
            discardedConsumerIds.remove(consumerId)
        }
    }
    
    private func addProduceContinuation(_ continuation: CheckedContinuation<Output, Error>, producerId: UInt64) {
        lock.synchronized {
            produceContinuations[producerId] = continuation
        }
    }
    
    @discardableResult
    private func removeProduceContinuation(producerId: UInt64) -> CheckedContinuation<Output, Error>? {
        lock.synchronized {
            produceContinuations.removeValue(forKey: producerId)
        }
    }
    
    private func startTransmissionsIfRequired() {
        lock.synchronized {
            var retainedSupplies: [Supply] = []
            while true {
                guard let supply = dequeueSupply() else { break }
                switch mode {
                case .cumulative:
                    if !hasConsumerFor(supply: supply) {
                        retainedSupplies.append(supply)
                        continue
                    }
                case .retainable:
                    if !hasConsumerFor(supply: supply) {
                        discardProducer(producerId: supply.producerId)
                        continue
                    }
                case .transient:
                    if !hasNontransmittingConsumerFor(supply: supply) {
                        discardProducer(producerId: supply.producerId)
                        continue
                    }
                }
                if case .broadcast = scheme, !allConsumersAreReadyFor(input: supply.input) {
                    retainedSupplies.append(supply)
                    continue
                }
                var supplyIsConsumed = false
                demandLoop: while true {
                    guard let demand = dequeueDemand(supply: supply) else {
                        break
                    }
                    if let consumeContinuation = removeConsumeContinuation(consumerId: demand.consumerId) {
                        consumeContinuation.resume(returning: supply.input)
                        beginTransmission(producerId: supply.producerId, consumerId: demand.consumerId)
                        supplyIsConsumed = true
                        switch scheme {
                        case .unicast, .anycast:
                            break demandLoop
                        case .multicast, .broadcast:
                            continue demandLoop
                        }
                    }
                }
                if !supplyIsConsumed {
                    retainedSupplies.append(supply)
                }
            }
            for retainedSupply in retainedSupplies {
                queueSupply(retainedSupply)
            }
        }
    }
    
    private func finishTransmissionsIfRequired() {
        lock.synchronized {
            for producerId in produceContinuations.keys {
                if allRepliesAreReady(producerId: producerId) {
                    if let produceContinuation = removeProduceContinuation(producerId: producerId) {
                        let consumerIds = endTransactions(producerId: producerId).map({ $0.consumerId })
                        let collectedReplies = collectReplies(consumerIds: consumerIds)
                        var outputs: [Output] = []
                        var firstError: Error? = nil
                        replyLoop: for collectedReply in collectedReplies {
                            switch collectedReply.result {
                            case .failure(let error):
                                firstError = error
                                break replyLoop
                            case .success(let output):
                                outputs.append(output)
                            }
                        }
                        if let firstError = firstError {
                            produceContinuation.resume(throwing: firstError)
                        } else {
                            switch scheme {
                            case .unicast:
                                produceContinuation.resume(returning: outputs.first!)
                            case .broadcast(let combiner):
                                produceContinuation.resume(returning: combiner?(outputs) ?? outputs.last!)
                            case .multicast(let combiner):
                                produceContinuation.resume(returning: combiner?(outputs) ?? outputs.last!)
                            case .anycast:
                                produceContinuation.resume(returning: outputs.first!)
                            }
                        }
                    }
                }
            }
        }
    }
    
    private func allRepliesAreReady(producerId: UInt64) -> Bool {
        lock.synchronized {
            let producerTransmissions = transmissions.filter({ $0.producerId == producerId })
            if producerTransmissions.isEmpty { return false }
            return producerTransmissions.allSatisfy({ transmission in
                replyCollection.contains(where: { $0.consumerId == transmission.consumerId })
            })
        }
    }
    
    private func hasConsumer(consumerId: UInt64) -> Bool {
        lock.synchronized {
            attachedConsumerIds.contains(where: { attachedConsumerId, _ in attachedConsumerId == consumerId })
        }
    }
    
    private func hasConsumerFor(supply: Supply) -> Bool {
        lock.synchronized {
            if case .unicast = scheme {
                return attachedConsumerIds.contains(where: { consumerId, inputSpec in consumerId == supply.producerId && inputSpec.isSatisfiedBy(supply.input) })
            } else {
                return attachedConsumerIds.values.contains(where: { $0.isSatisfiedBy(supply.input) })
            }
        }
    }
    
    private func hasNontransmittingConsumerFor(supply: Supply) -> Bool {
        lock.synchronized {
            if case .unicast = scheme {
                return attachedConsumerIds.contains(where: { consumerId, inputSpec in consumerId == supply.producerId && inputSpec.isSatisfiedBy(supply.input) && !transmissions.contains { $0.consumerId == consumerId } })
            } else {
                return attachedConsumerIds.contains(where: { consumerId, inputSpec in inputSpec.isSatisfiedBy(supply.input) && !transmissions.contains { $0.consumerId == consumerId } })
            }
        }
    }
    
    private func allConsumersAreReadyFor(input: Input) -> Bool {
        lock.synchronized {
            attachedConsumerIds.filter({ _, spec in spec.isSatisfiedBy(input) }).keys.allSatisfy({ attachedConsumerId in
                demandQueue.contains(where: { $0.consumerId == attachedConsumerId && $0.inputSpec.isSatisfiedBy(input) })
            })
        }
    }
    
    private func queueDemand(_ demand: Demand) {
        lock.synchronized {
            demandQueue.append(demand)
        }
    }
    
    private func dequeueDemand(consumerId: UInt64) -> Demand? {
        lock.synchronized {
            demandQueue.removeFirst(where: { $0.consumerId == consumerId })
        }
    }
    
    private func dequeueDemand(supply: Supply) -> Demand? {
        lock.synchronized {
            if case .unicast = scheme {
                return demandQueue.removeFirst(where: { $0.consumerId == supply.producerId && $0.inputSpec.isSatisfiedBy(supply.input) })
            } else {
                return demandQueue.removeFirst(where: { $0.inputSpec.isSatisfiedBy(supply.input) })
            }
        }
    }
    
    private func queueSupply(_ supply: Supply) {
        lock.synchronized {
            while supplyQueue.count >= mode.capacity {
                if let supply = supplyQueue.popFirst() {
                    discardProducer(producerId: supply.producerId)
                }
            }
            supplyQueue.append(supply)
        }
    }
    
    private func dequeueSupply(producerId: UInt64) -> Supply? {
        lock.synchronized {
            supplyQueue.removeFirst(where: { $0.producerId == producerId })
        }
    }
    
    private func dequeueSupply(spec: TSpec<Input>) -> Supply? {
        lock.synchronized {
            supplyQueue.removeFirst(where: { spec.isSatisfiedBy($0.input) })
        }
    }
    
    private func dequeueSupply() -> Supply? {
        lock.synchronized {
            supplyQueue.popFirst()
        }
    }
    
    private func addReply(_ reply: Reply) {
        lock.synchronized {
            replyCollection.append(reply)
            finishTransmissionsIfRequired()
        }
    }
    
    private func collectReplies(consumerIds: [UInt64]) -> [Reply] {
        lock.synchronized {
            let index = replyCollection.partition(by: { consumerIds.contains($0.consumerId) })
            if index == replyCollection.count { return [] }
            let collectingReplies = replyCollection[index...]
            replyCollection = Array(replyCollection[..<index])
            return Array(collectingReplies)
        }
    }
    
    private func checkSeal() throws {
        try lock.synchronized {
            if let sealError = sealError {
                throw sealError
            }
        }
    }
    
}

public extension Gate {
    
    var asSource: AnySource<Input, Output> {
        AnySource(gate: self)
    }
    
    var asDrain: AnyDrain<Input, Output> {
        AnyDrain(gate: self)
    }
    
}
