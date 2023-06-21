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

@preconcurrency import Foundation

public func withCancellableCheckedThrowingContinuation<T>(
    function: String = #function,
    _ body: (CheckedContinuation<T, Error>, Cancellation) -> Void
) async throws -> T {
    let cancellation = Cancellation()
    return try await withTaskCancellationHandler {
        return try await withCheckedThrowingContinuation(function: function) { (continuation: CheckedContinuation<T, Error>) -> Void in
            body(continuation, cancellation)
        }
    } onCancel: {
        cancellation.onCancel()
    }
}

public func withCancellableCheckedContinuation<T>(
    function: String = #function,
    _ body: (CheckedContinuation<T, Never>, Cancellation) -> Void
) async -> T {
    let cancellation = Cancellation()
    return await withTaskCancellationHandler {
        return await withCheckedContinuation(function: function) { (continuation: CheckedContinuation<T, Never>) -> Void in
            body(continuation, cancellation)
        }
    } onCancel: {
        cancellation.onCancel()
    }
}

public struct Cancellation: Sendable {
    
    @UncheckedReference private var internalOnCancel: @Sendable () -> Void
    @UncheckedReference private var canceled: Bool
    private let lock: NSLock
    
    init() {
        let lock = NSLock()
        let canceled = UncheckedReference(wrappedValue: false)
        let internalOnCancel = UncheckedReference(wrappedValue: { @Sendable in lock.withLock { canceled =^ true } })
        _internalOnCancel = internalOnCancel
        _canceled = canceled
        self.lock = lock
    }
    
    public var onCancel: @Sendable () -> Void {
        get {
            lock.withLock { internalOnCancel }
        }
        nonmutating set {
            if lock.withLock({ $internalOnCancel =^ newValue; return canceled }) { internalOnCancel() }
        }
    }
    
}

@discardableResult
public func runBlocking<R>(_ operation: @Sendable @escaping () async -> R) -> R {
    let semaphore = DispatchSemaphore(value: 0)
    @UncheckedReference var result: R? = nil
    let priority = Task.currentPriority
    Task.detached(priority: priority) {
        await $result =^ operation()
        semaphore.signal()
    }
    semaphore.wait()
    return result!
}

@discardableResult
public func runBlocking<R>(_ operation: @Sendable @escaping () async throws -> R) throws -> R {
    let semaphore = DispatchSemaphore(value: 0)
    @UncheckedReference var result: Result<R, Error>? = nil
    let priority = Task.currentPriority
    Task.detached(priority: priority) {
        do {
            $result =^ .success(try await operation())
        } catch {
            $result =^ .failure(error)
        }
        semaphore.signal()
    }
    semaphore.wait()
    switch result! {
    case .success(let r):
        return r
    case .failure(let error):
        throw error
    }
}

@discardableResult
public func runBlocking<R>(timeout: DispatchTime, _ operation: @Sendable @escaping () async throws -> R) throws -> R {
    let semaphore = DispatchSemaphore(value: 0)
    @UncheckedReference var result: Result<R, Error>? = nil
    let priority = Task.currentPriority
    Task.detached(priority: priority) {
        do {
            $result =^ .success(try await operation())
        } catch {
            $result =^ .failure(error)
        }
        semaphore.signal()
    }
    switch semaphore.wait(timeout: timeout) {
    case .success:
        switch result! {
        case .success(let r):
            return r
        case .failure(let error):
            throw error
        }
    case .timedOut:
        throw TimedOutError()
    }
}

@discardableResult
public func runBlocking<R>(wallTimeout: DispatchWallTime, _ operation: @Sendable @escaping () async throws -> R) throws -> R {
    let semaphore = DispatchSemaphore(value: 0)
    @UncheckedReference var result: Result<R, Error>? = nil
    let priority = Task.currentPriority
    Task.detached(priority: priority) {
        do {
            $result =^ .success(try await operation())
        } catch {
            $result =^ .failure(error)
        }
        semaphore.signal()
    }
    switch semaphore.wait(wallTimeout: wallTimeout) {
    case .success:
        switch result! {
        case .success(let r):
            return r
        case .failure(let error):
            throw error
        }
    case .timedOut:
        throw TimedOutError()
    }
}

struct TimedOutError: LocalizedError {
    public var errorDescription: String? { "Operation is interrupted because timed out!" }
}

public extension AsyncStream {
    
    init(unfolding produce: @escaping () async -> Element?, onTerminate: @escaping () -> Void, onCancel: (@Sendable () -> Void)? = nil) {
        var terminated = false
        let produceWithTermination: () async -> Element? = {
            if terminated {
                return nil
            }
            let element = await produce()
            if element == nil {
                onTerminate()
                terminated = true
            }
            return element
        }
        self.init(unfolding: produceWithTermination, onCancel: onCancel)
    }
    
}

public extension AsyncThrowingStream {
    
    init(unfolding produce: @escaping () async throws -> Element?, onTerminate: @escaping () -> Void) where Failure == Error {
        var terminated = false
        let produceWithTermination: () async throws -> Element? = {
            if terminated {
                return nil
            }
            do {
                let element = try await produce()
                if element == nil {
                    onTerminate()
                    terminated = true
                }
                return element
            } catch {
                onTerminate()
                terminated = true
                throw error
            }
        }
        self.init(unfolding: produceWithTermination)
    }
    
}

public struct AsyncRethrowingStream<Element, I: AsyncIteratorProtocol>: AsyncSequence where I.Element == Element {
    
    public final class Iterator<Element, I: AsyncIteratorProtocol>: AsyncIteratorProtocol where I.Element == Element {
        
        private var iterator: I
        private let onTerminate: (() -> Void)?
        public private(set) var terminated = false
        
        fileprivate init(iterator: I, onTerminate: (() -> Void)?) {
            self.iterator = iterator
            self.onTerminate = onTerminate
        }
        
        deinit {
            if !terminated {
                onTerminate?()
            }
        }
        
        public func next() async rethrows -> Element? {
            if terminated {
                return nil
            }
            do {
                let element = try await iterator.next()
                if element == nil {
                    onTerminate?()
                    terminated = true
                }
                return element
            } catch {
                onTerminate?()
                terminated = true
                throw error
            }
        }
        
    }
    
    private let iterator: Iterator<Element, I>
    
    public init(iterator: I, onTerminate: (() -> Void)? = nil) {
        self.iterator = Iterator(iterator: iterator, onTerminate: onTerminate)
    }
    
    public var terminated: Bool {
        iterator.terminated
    }

    public func makeAsyncIterator() -> Iterator<Element, I> {
        iterator
    }
    
}

postfix operator *?

public postfix func *?<S: AsyncSequence>(_ sequence: S) -> AsyncStream<S.Element> {
    var iterator = sequence.makeAsyncIterator()
    return AsyncStream(unfolding: { try? await iterator.next() })
}

postfix operator *!

public postfix func *!<S: AsyncSequence>(_ sequence: S) -> AsyncThrowingStream<S.Element, Error> {
    var iterator = sequence.makeAsyncIterator()
    return AsyncThrowingStream(unfolding: { try await iterator.next() })
}

postfix operator *~

public postfix func *~<S: AsyncSequence>(_ sequence: S) -> AsyncRethrowingStream<S.Element, S.AsyncIterator> {
    let iterator = sequence.makeAsyncIterator()
    return AsyncRethrowingStream(iterator: iterator)
}

public func iterate<Element>(cancelable: Bool = true, elements: [Element]) -> AsyncStream<Element> {
    let count = elements.count
    var i = 0
    let next: () async -> Element? = {
        if i >= count || (cancelable && Task.isCancelled) {
            return nil
        }
        defer { i += 1 }
        return elements[i]
    }
    return AsyncStream(unfolding: next)
}

public func iterate<Element>(cancelable: Bool = true, elements: Element...) -> AsyncStream<Element> {
    let count = elements.count
    var i = 0
    let next: () async -> Element? = {
        if i >= count || (cancelable && Task.isCancelled) {
            return nil
        }
        defer { i += 1 }
        return elements[i]
    }
    return AsyncStream(unfolding: next)
}

public func iterate<Element>(cancelable: Bool = true, count: Int, element: @autoclosure @escaping () -> Element) -> AsyncStream<Element> {
    var i = 0
    let next: () async -> Element? = {
        if i >= count || (cancelable && Task.isCancelled) {
            return nil
        }
        defer { i += 1 }
        return element()
    }
    return AsyncStream(unfolding: next)
}

public func iterate(cancelable: Bool = true, count: Int) -> AsyncStream<Void> {
    iterate(cancelable: cancelable, count: count, element: ())
}

public func iterateInfinitely<Element>(element: @autoclosure @escaping () -> Element) -> AsyncStream<Element> {
    let next: () async -> Element? = {
        if Task.isCancelled {
            return nil
        }
        return element()
    }
    return AsyncStream(unfolding: next)
}

public func iterateInfinitely() -> AsyncStream<Void> {
    iterateInfinitely(element: ())
}

public extension AsyncSequence {
    
    func debounce(interval: Duration) -> AsyncThrowingStream<Element, Error> where Element: Sendable, AsyncIterator: Sendable {
        let provider = DebounceProvider(debounceInterval: interval, iterator: self.makeAsyncIterator(), clock: .continuous)
        return AsyncThrowingStream(unfolding: provider.next)
    }
    
}

fileprivate final class DebounceProvider<Element: Sendable> {
    
    private var terminated = false
    private let gate: Gate<Result<Element?, Error>, Void>
    private let tasks: [Task<Void, Never>]
    
    fileprivate init<I: AsyncIteratorProtocol & Sendable, C: Clock>(debounceInterval: C.Duration, iterator: I, clock: C) where I.Element == Element {
        let gate = Gate<Result<Element?, Error>, Void>(mode: .cumulative(), scheme: .anycast)
        self.gate = gate
        @UncheckedReference var candidateTimestamp: C.Instant? = nil
        @UncheckedReference var candidateElement: Element? = nil
        let mutex = Mutex()
        let semaphore = Semaphore(initialPermits: 0)
        tasks = [
            Task<Void, Never>.detached {
                var iterator = iterator
                while !Task.isCancelled {
                    @UncheckedReference var element: Element? = nil
                    do {
                        try await $element =^ iterator.next()
                        await mutex.atomic {
                            if element == nil {
                                try? await gate.send(.success(nil))
                                gate.seal()
                            } else {
                                $candidateTimestamp =^ clock.now
                                $candidateElement =^ element
                            }
                        }
                    } catch {
                        await mutex.atomic {
                            try? await gate.send(.failure(error))
                            gate.seal()
                        }
                    }
                    try? semaphore.signal()
                }
            },
            Task<Void, Never>.detached {
                while !Task.isCancelled {
                    @UncheckedReference var sleepInterval: C.Duration? = nil
                    await mutex.atomic {
                        guard let candidateInterval: C.Duration = letNotNil(candidateTimestamp, { $0.duration(to: clock.now) }) , let element: Element = candidateElement else {
                            $sleepInterval =^ nil
                            return
                        }
                        if candidateInterval >= debounceInterval {
                            try? await gate.send(.success(element))
                            $candidateTimestamp =^ nil
                            $candidateElement =^ nil
                        }
                        $sleepInterval =^ max(C.Duration.zero, debounceInterval - candidateInterval)
                    }
                    if let sleepInterval {
                        try? await clock.sleep(until: clock.now.advanced(by: sleepInterval), tolerance: nil)
                    } else {
                        try? await semaphore.await()
                    }
                }
            }
        ]
    }
    
    func next() async throws -> Element? {
        guard !terminated else { return nil }
        switch try? await gate.receive() {
        case .success(let element):
            if element == nil {
                terminateUnsafe()
            }
            return element
        case .failure(let error):
            terminateUnsafe()
            throw error
        case .none:
            terminateUnsafe()
            return nil
        }
    }
    
    private func terminateUnsafe() {
        terminated = true
        tasks.forEach({ $0.cancel() })
    }
    
}
