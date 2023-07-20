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

public enum BarrierError: Error {
    case disabledBarrier
    case resetBarrier
}

extension BarrierError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .disabledBarrier:
            return "Barrier is disabled"
        case .resetBarrier:
            return "Barrier is reset"
        }
    }
}

public final class Barrier: @unchecked Sendable {
    
    public let requiredParties: Int
    private var enabled: Bool
    private var continuations: [CheckedContinuation<Void, Error>] = []
    private let lock = NSRecursiveLock()
    
    public init(requiredParties: Int, enabled: Bool = true) {
        self.requiredParties = requiredParties
        self.enabled = enabled
    }
    
    public var awaitingParties: Int {
        lock.withLock { continuations.count }
    }
    
    public var isEnabled: Bool {
        lock.withLock { enabled }
    }
    
    public func withTransaction(_ transaction: (Barrier) -> Void) {
        lock.withLock { transaction(self) }
    }
    
    public func await(enabledAfterCancellation: Bool = false) async throws {
        try await withCancellableCheckedThrowingContinuation() { [weak self] (continuation: CheckedContinuation<Void, Error>, cancellation: Cancellation) -> Void in
            self?.withTransaction { barrier in
                guard barrier.enabled else {
                    continuation.resume(throwing: BarrierError.disabledBarrier)
                    return
                }
                barrier.enqueueContinuationUnsafe(continuation)
                barrier.resolveContinuationsUnsafe()
                cancellation.onCancel = { [weak barrier] in
                    barrier?.reset(enabled: enabledAfterCancellation)
                }
            }
        }
    }
    
    public func reset(enabled: Bool, error: Error = BarrierError.resetBarrier) {
        lock.withLock {
            self.enabled = enabled
            for continuation in continuations {
                continuation.resume(throwing: error)
            }
            continuations.removeAll()
        }
    }
    
    private func enqueueContinuationUnsafe(_ continuation: CheckedContinuation<Void, Error>) {
        continuations.append(continuation)
    }
    
    private func resolveContinuationsUnsafe() {
        if continuations.count < requiredParties { return }
        for continuation in continuations {
            continuation.resume()
        }
        continuations.removeAll()
    }
    
}
