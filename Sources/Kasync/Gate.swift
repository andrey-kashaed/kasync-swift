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

public protocol Drain<Input, Output>: Sendable {
    
    associatedtype Input
    associatedtype Output
    
    var isSealed: Bool { get }
    
    @discardableResult
    func send(producerId: UInt64, _ provider: @escaping () async throws -> Input) async throws -> Output
    
    func sender(producerId: UInt64, provider: AsyncThrowingStream<Input, Error>) -> AsyncThrowingStream<Output, Error>
    
}

public extension Drain {
    
    @discardableResult
    func send(producerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), _ provider: @autoclosure @escaping () async throws -> Input) async throws -> Output {
        try await send(producerId: producerId, provider)
    }
    
    @discardableResult
    func send(producerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), _ provider: @escaping () async throws -> Input) async throws -> Output {
        try await send(producerId: producerId, provider)
    }

    func sender(producerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), provider: AsyncThrowingStream<Input, Error>) -> AsyncThrowingStream<Output, Error> {
        sender(producerId: producerId, provider: provider)
    }
    
}

public extension Drain {

    @discardableResult
    func send<Element>(producerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), _ provider: Element...) async throws -> Output where Input == Array<Element> {
        return try await send(producerId: producerId, provider)
    }
    
}

public final class ConfinedDrain<Input, Output>: Drain {
    
    private let gate: Gate<Input, Output>
    
    fileprivate init(gate: Gate<Input, Output>) {
        self.gate = gate
    }
    
    public var isSealed: Bool {
        gate.isSealed
    }
    
    @discardableResult
    public func send(producerId: UInt64, _ provider: @escaping () async throws -> Input) async throws -> Output {
        try await gate.send(producerId: producerId, provider)
    }
    
    public func sender(producerId: UInt64, provider: AsyncThrowingStream<Input, Error>) -> AsyncThrowingStream<Output, Error> {
        gate.sender(producerId: producerId, provider: provider)
    }
    
}

public protocol Source<Input, Output>: Sendable {
    
    associatedtype Input
    associatedtype Output
    
    var isSealed: Bool { get }
    
    @discardableResult
    func process(consumerId: UInt64, spec: any Spec<Input>, operation: (Input) async throws -> Output) async throws -> Output
    
    @discardableResult
    func receive(consumerId: UInt64, spec: any Spec<Input>, instantOutput: Output) async throws -> Input
    
    func processor(consumerId: UInt64, spec: any Spec<Input>, operation: @escaping (Input) async throws -> Output) -> AsyncThrowingStream<Output, Error>
    
    func receiver(consumerId: UInt64, spec: any Spec<Input>, instantOutput: Output) -> AsyncThrowingStream<Input, Error>
    
}

public extension Source {
    
    @discardableResult
    func process(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), spec: any Spec<Input> = trueSpec(), operation: (Input) async throws -> Output) async throws -> Output {
        try await process(consumerId: consumerId, spec: spec, operation: operation)
    }
    
    @discardableResult
    func process<SubInput>(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), operation: (SubInput) async throws -> Output) async throws -> Output {
        try await process(consumerId: consumerId, spec: spec(isSatisfiedBy: { $0 is SubInput }), operation: { try await operation($0 as! SubInput) })
    }
    
    @discardableResult
    func receive(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), spec: any Spec<Input> = trueSpec(), instantOutput: Output) async throws -> Input {
        try await receive(consumerId: consumerId, spec: spec, instantOutput: instantOutput)
    }
    
    @discardableResult
    func receive<SubInput>(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), instantOutput: Output) async throws -> SubInput {
        try await receive(consumerId: consumerId, spec: spec(isSatisfiedBy: { $0 is SubInput }), instantOutput: instantOutput) as! SubInput
    }
    
    func processor(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), spec: any Spec<Input> = trueSpec(), operation: @escaping (Input) async throws -> Output) -> AsyncThrowingStream<Output, Error> {
        processor(consumerId: consumerId, spec: spec, operation: operation)
    }
    
    func receiver(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), spec: any Spec<Input> = trueSpec(), instantOutput: Output) -> AsyncThrowingStream<Input, Error> {
        receiver(consumerId: consumerId, spec: spec, instantOutput: instantOutput)
    }
    
    func processor<SubInput>(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), operation: @escaping (SubInput) async throws -> Output) -> AsyncThrowingStream<Output, Error> {
        processor(consumerId: consumerId, spec: spec(isSatisfiedBy: { $0 is SubInput }), operation: { try await operation($0 as! SubInput) })
    }
    
    func receiver<SubInput>(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), instantOutput: Output) -> AsyncThrowingStream<SubInput, Error> {
        receiver(consumerId: consumerId, spec: spec(isSatisfiedBy: { $0 is SubInput }), instantOutput: instantOutput).map { $0 as! SubInput }*!
    }
    
}

public extension Source where Output == Void {
    
    @discardableResult
    func receive(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), spec: any Spec<Input> = trueSpec()) async throws -> Input {
        try await receive(consumerId: consumerId, spec: spec, instantOutput: ())
    }
    
    @discardableResult
    func receive<SubInput>(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max)) async throws -> SubInput {
        try await receive(consumerId: consumerId, instantOutput: ())
    }
    
    func receiver(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max), spec: any Spec<Input> = trueSpec()) -> AsyncThrowingStream<Input, Error> {
        receiver(consumerId: consumerId, spec: spec, instantOutput: ())
    }
    
    func receiver<SubInput>(consumerId: UInt64 = UInt64.random(in: UInt64.min...UInt64.max)) -> AsyncThrowingStream<SubInput, Error> {
        receiver(consumerId: consumerId, instantOutput: ())
    }
    
}

public final class ConfinedSource<Input, Output>: Source {
    
    private let gate: Gate<Input, Output>
    
    fileprivate init(gate: Gate<Input, Output>) {
        self.gate = gate
    }
    
    public var isSealed: Bool {
        gate.isSealed
    }
    
    @discardableResult
    public func process(consumerId: UInt64, spec: any Spec<Input>, operation: (Input) async throws -> Output) async throws -> Output {
        try await gate.process(consumerId: consumerId, spec: spec, operation: operation)
    }
    
    @discardableResult
    public func receive(consumerId: UInt64, spec: any Spec<Input>, instantOutput: Output) async throws -> Input {
        try await gate.receive(consumerId: consumerId, spec: spec, instantOutput: instantOutput)
    }
    
    public func processor(consumerId: UInt64, spec: any Spec<Input>, operation: @escaping (Input) async throws -> Output) -> AsyncThrowingStream<Output, Error> {
        gate.processor(consumerId: consumerId, spec: spec, operation: operation)
    }
    
    public func receiver(consumerId: UInt64, spec: any Spec<Input>, instantOutput: Output) -> AsyncThrowingStream<Input, Error> {
        gate.receiver(consumerId: consumerId, spec: spec, instantOutput: instantOutput)
    }
    
}

public enum GateError: Error {
    case sealedGate
    case canceledProducer
    case discardedProducer
    case canceledConsumer
    case discardedConsumer
}

extension GateError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .sealedGate:
            return "Gate is sealed"
        case .canceledProducer:
            return "Producer is canceled"
        case .discardedProducer:
            return "Producer is discarded"
        case .canceledConsumer:
            return "Consumer is canceled"
        case .discardedConsumer:
            return "Consumer is discarded"
        }
    }
}

public final class Gate<Input, Output>: Source, Drain, CustomDebugStringConvertible, @unchecked Sendable {
    
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
        let inputSpec: any Spec<Input>
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
    private var producerContinuations: [UInt64: CheckedContinuation<Output, Error>] = [:]
    private var consumerContinuations: [UInt64: CheckedContinuation<Input, Error>] = [:]
    private var attachedConsumerIds: [UInt64: any Spec<Input>] = [:]
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
        lock.withLock {
            "\(type(of: self)) demandQueue.count: \(demandQueue.count), supplyQueue.count: \(supplyQueue.count), producerContinuations.count: \(producerContinuations.count), consumerContinuations.count: \(consumerContinuations.count), replyCollection.count: \(replyCollection.count), transmissions.count: \(transmissions.count)"
        }
    }
    
    public func seal(_ error: Error) {
        lock.withLock {
            sealError = error
            demandQueue.removeAll()
            supplyQueue.removeAll()
            replyCollection.removeAll()
            consumerContinuations.values.forEach { consumerContinuation in
                consumerContinuation.resume(throwing: error)
            }
            consumerContinuations.removeAll()
            producerContinuations.values.forEach { producerContinuation in
                producerContinuation.resume(throwing: error)
            }
            producerContinuations.removeAll()
            transmissions.removeAll()
        }
    }
    
    public var isSealed: Bool {
        lock.withLock { sealError != nil }
    }
    
    public func discardProducer(producerId: UInt64) {
        lock.withLock {
            let _ = dequeueSupply(producerId: producerId)
            guard let producerContinuation = removeProducerContinuation(producerId: producerId) else {
                return
            }
            producerContinuation.resume(throwing: GateError.discardedProducer)
        }
    }
    
    public func discardConsumer(consumerId: UInt64) {
        lock.withLock {
            let _ = dequeueDemand(consumerId: consumerId)
            if let consumerContinuation = removeConsumerContinuation(consumerId: consumerId) {
                consumerContinuation.resume(throwing: GateError.discardedConsumer)
                return
            }
            if hasConsumer(consumerId: consumerId) {
                addDiscardedConsumerId(consumerId)
            }
        }
    }
    
    public func sender(producerId: UInt64, provider: AsyncThrowingStream<Input, Error>) -> AsyncThrowingStream<Output, Error> {
        var providerIterator = provider.makeAsyncIterator()
        return AsyncThrowingStream(
            unfolding: { [weak self] in
                guard let input = try await providerIterator.next() else { return nil }
                return try await self?.send(producerId: producerId, { input })
            }
        )
    }
    
    @discardableResult
    public func send(producerId: UInt64, _ provider: @escaping () async throws -> Input) async throws -> Output {
        let input = try await provider()
        return try await produce(input, producerId: producerId)
    }
    
    public func processor(consumerId: UInt64, spec: any Spec<Input>, operation: @escaping (Input) async throws -> Output) -> AsyncThrowingStream<Output, Error> {
        attachConsumerId(consumerId, inputSpec: spec)
        return AsyncThrowingStream(
            unfolding: { [weak self] in
                try await self?.process(spec: spec, operation: operation, consumerId: consumerId)
            },
            onTerminate: { [weak self] in
                self?.detachConsumerId(consumerId)
            }
        )
    }
    
    public func receiver(consumerId: UInt64, spec: any Spec<Input>, instantOutput: Output) -> AsyncThrowingStream<Input, Error> {
        attachConsumerId(consumerId, inputSpec: spec)
        return AsyncThrowingStream(
            unfolding: { [weak self] in
                try await self?.receive(spec: spec, instantOutput: instantOutput, consumerId: consumerId)
            },
            onTerminate: { [weak self] in
                self?.detachConsumerId(consumerId)
            }
        )
    }
    
    @discardableResult
    public func process(consumerId: UInt64, spec: any Spec<Input>, operation: (Input) async throws -> Output) async throws -> Output {
        attachConsumerId(consumerId, inputSpec: spec)
        defer { detachConsumerId(consumerId) }
        return try await process(spec: spec, operation: operation, consumerId: consumerId)
    }
    
    @discardableResult
    public func receive(consumerId: UInt64, spec: any Spec<Input>, instantOutput: Output) async throws -> Input {
        attachConsumerId(consumerId, inputSpec: spec)
        defer { detachConsumerId(consumerId) }
        return try await receive(spec: spec, instantOutput: instantOutput, consumerId: consumerId)
    }
    
    @discardableResult
    private func process(spec: any Spec<Input>, operation: (Input) async throws -> Output, consumerId: UInt64) async throws -> Output {
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
    private func receive(spec: any Spec<Input>, instantOutput: Output, consumerId: UInt64) async throws -> Input {
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
        let output = try await withCancellableCheckedThrowingContinuation() { [weak self] (continuation: CheckedContinuation<Output, Error>, cancellation: Cancellation) -> Void in
            self?.withTransaction { gate in
                gate.addProducerContinuation(continuation, producerId: producerId)
                gate.enqueueSupply(Supply(producerId: producerId, input: input))
                gate.startTransmissionsIfRequired()
                cancellation.onCancel = { [weak gate] in
                    gate?.removeProducerContinuation(producerId: producerId)?.resume(throwing: GateError.canceledProducer)
                }
            }
        }
        return output
    }
    
    private func consume(_ inputSpec: any Spec<Input>, consumerId: UInt64) async throws -> Input {
        try checkSeal()
        let input = try await withCancellableCheckedThrowingContinuation() { [weak self] (continuation: CheckedContinuation<Input, Error>, cancellation: Cancellation) -> Void in
            self?.withTransaction { gate in
                if gate.hasDiscardedConsumerId(consumerId) {
                    gate.removeDiscardedConsumerId(consumerId)
                    continuation.resume(throwing: GateError.discardedConsumer)
                    return
                }
                gate.addConsumerContinuation(continuation, consumerId: consumerId)
                gate.enqueueDemand(Demand(consumerId: consumerId, inputSpec: inputSpec))
                gate.startTransmissionsIfRequired()
                cancellation.onCancel = { [weak self] in
                    self?.removeConsumerContinuation(consumerId: consumerId)?.resume(throwing: GateError.canceledConsumer)
                }
            }
        }
        return input
    }
    
    private func respond(consumerId: UInt64, result: Result<Output, Error>) {
        lock.withLock {
            if transmissions.contains(where: { $0.consumerId == consumerId }) {
                addReply(Reply(consumerId: consumerId, result: result))
            }
        }
    }
    
    private func beginTransmission(producerId: UInt64, consumerId: UInt64) {
        lock.withLock {
            transmissions.append(Transmission(producerId: producerId, consumerId: consumerId))
        }
    }
    
    private func endTransactions(producerId: UInt64) -> [Transmission] {
        lock.withLock {
            let index = transmissions.partition(by: { $0.producerId == producerId })
            if index == transmissions.count { return [] }
            let endingTransmissions = transmissions[index...]
            transmissions = Array(transmissions[..<index])
            return Array(endingTransmissions)
        }
    }
    
    private func hasConsumerContinuation() -> Bool {
        lock.withLock {
            !consumerContinuations.isEmpty
        }
    }
    
    private func addConsumerContinuation(_ continuation: CheckedContinuation<Input, Error>, consumerId: UInt64) {
        lock.withLock {
            consumerContinuations[consumerId] = continuation
        }
    }
    
    @discardableResult
    private func removeConsumerContinuation(consumerId: UInt64) -> CheckedContinuation<Input, Error>? {
        lock.withLock {
            consumerContinuations.removeValue(forKey: consumerId)
        }
    }
    
    private func attachConsumerId(_ consumerId: UInt64, inputSpec: any Spec<Input>) {
        lock.withLock {
            attachedConsumerIds[consumerId] = inputSpec
        }
    }
    
    private func detachConsumerId(_ consumerId: UInt64) {
        lock.withLock {
            attachedConsumerIds.removeValue(forKey: consumerId)
            removeDiscardedConsumerId(consumerId)
        }
    }
    
    private func hasDiscardedConsumerId(_ consumerId: UInt64) -> Bool {
        lock.withLock {
            discardedConsumerIds.contains(consumerId)
        }
    }
    
    private func addDiscardedConsumerId(_ consumerId: UInt64) {
        lock.withLock {
            discardedConsumerIds.append(consumerId)
        }
    }
    
    private func removeDiscardedConsumerId(_ consumerId: UInt64) {
        lock.withLock {
            let _ = discardedConsumerIds.remove(consumerId)
        }
    }
    
    private func addProducerContinuation(_ continuation: CheckedContinuation<Output, Error>, producerId: UInt64) {
        lock.withLock {
            producerContinuations[producerId] = continuation
        }
    }
    
    @discardableResult
    private func removeProducerContinuation(producerId: UInt64) -> CheckedContinuation<Output, Error>? {
        lock.withLock {
            producerContinuations.removeValue(forKey: producerId)
        }
    }
    
    private func startTransmissionsIfRequired() {
        lock.withLock {
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
                    if let consumerContinuation = removeConsumerContinuation(consumerId: demand.consumerId) {
                        consumerContinuation.resume(returning: supply.input)
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
                enqueueSupply(retainedSupply)
            }
        }
    }
    
    private func finishTransmissionsIfRequired() {
        lock.withLock {
            for producerId in producerContinuations.keys {
                if allRepliesAreReady(producerId: producerId) {
                    if let producerContinuation = removeProducerContinuation(producerId: producerId) {
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
                            producerContinuation.resume(throwing: firstError)
                        } else {
                            switch scheme {
                            case .unicast:
                                producerContinuation.resume(returning: outputs.first!)
                            case .broadcast(let combiner):
                                producerContinuation.resume(returning: combiner?(outputs) ?? outputs.last!)
                            case .multicast(let combiner):
                                producerContinuation.resume(returning: combiner?(outputs) ?? outputs.last!)
                            case .anycast:
                                producerContinuation.resume(returning: outputs.first!)
                            }
                        }
                    }
                }
            }
        }
    }
    
    private func allRepliesAreReady(producerId: UInt64) -> Bool {
        lock.withLock {
            let producerTransmissions = transmissions.filter({ $0.producerId == producerId })
            if producerTransmissions.isEmpty { return false }
            return producerTransmissions.allSatisfy({ transmission in
                replyCollection.contains(where: { $0.consumerId == transmission.consumerId })
            })
        }
    }
    
    private func hasConsumer(consumerId: UInt64) -> Bool {
        lock.withLock {
            attachedConsumerIds.contains(where: { attachedConsumerId, _ in attachedConsumerId == consumerId })
        }
    }
    
    private func hasConsumerFor(supply: Supply) -> Bool {
        lock.withLock {
            if case .unicast = scheme {
                return attachedConsumerIds.contains(where: { consumerId, inputSpec in consumerId == supply.producerId && inputSpec.isSatisfiedBy(supply.input) })
            } else {
                return attachedConsumerIds.values.contains(where: { $0.isSatisfiedBy(supply.input) })
            }
        }
    }
    
    private func hasNontransmittingConsumerFor(supply: Supply) -> Bool {
        lock.withLock {
            if case .unicast = scheme {
                return attachedConsumerIds.contains(where: { consumerId, inputSpec in consumerId == supply.producerId && inputSpec.isSatisfiedBy(supply.input) && !transmissions.contains { $0.consumerId == consumerId } })
            } else {
                return attachedConsumerIds.contains(where: { consumerId, inputSpec in inputSpec.isSatisfiedBy(supply.input) && !transmissions.contains { $0.consumerId == consumerId } })
            }
        }
    }
    
    private func allConsumersAreReadyFor(input: Input) -> Bool {
        lock.withLock {
            attachedConsumerIds.filter({ _, spec in spec.isSatisfiedBy(input) }).keys.allSatisfy({ attachedConsumerId in
                demandQueue.contains(where: { $0.consumerId == attachedConsumerId && $0.inputSpec.isSatisfiedBy(input) })
            })
        }
    }
    
    private func enqueueDemand(_ demand: Demand) {
        lock.withLock {
            demandQueue.append(demand)
        }
    }
    
    private func dequeueDemand(consumerId: UInt64) -> Demand? {
        lock.withLock {
            demandQueue.removeFirst(where: { $0.consumerId == consumerId })
        }
    }
    
    private func dequeueDemand(supply: Supply) -> Demand? {
        lock.withLock {
            if case .unicast = scheme {
                return demandQueue.removeFirst(where: { $0.consumerId == supply.producerId && $0.inputSpec.isSatisfiedBy(supply.input) })
            } else {
                return demandQueue.removeFirst(where: { $0.inputSpec.isSatisfiedBy(supply.input) })
            }
        }
    }
    
    private func enqueueSupply(_ supply: Supply) {
        lock.withLock {
            while supplyQueue.count >= mode.capacity {
                if let supply = supplyQueue.popFirst() {
                    discardProducer(producerId: supply.producerId)
                }
            }
            supplyQueue.append(supply)
        }
    }
    
    private func dequeueSupply(producerId: UInt64) -> Supply? {
        lock.withLock {
            supplyQueue.removeFirst(where: { $0.producerId == producerId })
        }
    }
    
    private func dequeueSupply(spec: any Spec<Input>) -> Supply? {
        lock.withLock {
            supplyQueue.removeFirst(where: { spec.isSatisfiedBy($0.input) })
        }
    }
    
    private func dequeueSupply() -> Supply? {
        lock.withLock {
            supplyQueue.popFirst()
        }
    }
    
    private func addReply(_ reply: Reply) {
        lock.withLock {
            replyCollection.append(reply)
            finishTransmissionsIfRequired()
        }
    }
    
    private func collectReplies(consumerIds: [UInt64]) -> [Reply] {
        lock.withLock {
            let index = replyCollection.partition(by: { consumerIds.contains($0.consumerId) })
            if index == replyCollection.count { return [] }
            let collectingReplies = replyCollection[index...]
            replyCollection = Array(replyCollection[..<index])
            return Array(collectingReplies)
        }
    }
    
    private func checkSeal() throws {
        try lock.withLock {
            if let sealError = sealError {
                throw sealError
            }
        }
    }
    
    private func withTransaction(_ transaction: (Gate<Input, Output>) -> Void) {
        lock.withLock { transaction(self) }
    }
    
}

public extension Gate {
    
    var toSource: ConfinedSource<Input, Output> {
        ConfinedSource(gate: self)
    }
    
    var toDrain: ConfinedDrain<Input, Output> {
        ConfinedDrain(gate: self)
    }
    
}
