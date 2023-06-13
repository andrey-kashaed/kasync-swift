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
    case resetBarrier
    case finalizedBarrier
}

extension BarrierError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .resetBarrier:
            return "Barrier is reset"
        case .finalizedBarrier:
            return "Barrier is finalized"
        }
    }
}

public final class Barrier: @unchecked Sendable {
    
    private let count: Int
    private var enabled: Bool
    private var countdown: Int
    private var continuations: [CheckedContinuation<Void, Error>] = []
    private let lock = NSRecursiveLock()
    
    public init(count: Int, enabled: Bool = true) {
        self.count = count
        self.enabled = enabled
        self.countdown = count
    }
    
    public func await(enabledAfterCancellation: Bool = false) async throws {
        if lock.withLock({ !enabled }) { return }
        try await withCancellableCheckedThrowingContinuation() { [weak self] (continuation: CheckedContinuation<Void, Error>, cancellation: Cancellation) -> Void in
            cancellation.onCancel = {
                self?.reset(enabled: enabledAfterCancellation)
            }
            self?.addContinuation(continuation)
            self?.dispatchContinuations()
        }
    }
    
    public func reset(enabled: Bool, error: Error = BarrierError.resetBarrier) {
        lock.withLock {
            self.enabled = enabled
            countdown = count
            for continuation in continuations {
                continuation.resume(throwing: error)
            }
            continuations.removeAll()
        }
    }
    
    private func addContinuation(_ continuation: CheckedContinuation<Void, Error>) {
        lock.withLock {
            guard countdown > 0 else {
                continuation.resume(throwing: BarrierError.finalizedBarrier)
                return
            }
            continuations.append(continuation)
            countdown -= 1
        }
    }
    
    private func dispatchContinuations() {
        lock.withLock {
            if countdown > 0 { return }
            for continuation in continuations {
                continuation.resume()
            }
            continuations.removeAll()
        }
    }
    
}
